package akka.stream.burakkose.solr

import java.net.{ConnectException, SocketException}

import akka.NotUsed
import akka.stream.burakkose.solr.SolrFlowStage.{Finished, Idle, Sending}
import akka.stream.burakkose.solr.scaladsl.SolrSinkSettings
import akka.stream.stage.{GraphStage, InHandler, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.http.NoHttpResponseException
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.response.SolrResponseBase
import org.apache.solr.common.{SolrException, SolrInputDocument}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

final case class IncomingMessage[T](source: T)

final case class IncomingMessagesResult[T](sources: Seq[T], status: Int)

class SolrFlowStage[T](collection: String,
                       client: SolrClient,
                       settings: SolrSinkSettings,
                       messageBinder: T => SolrInputDocument)
    extends GraphStage[FlowShape[IncomingMessage[T], Future[IncomingMessagesResult[T]]]] {

  private val in = Inlet[IncomingMessage[T]]("messages")
  private val out = Outlet[Future[IncomingMessagesResult[T]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): SolrFlowLogic[T] =
    new SolrFlowLogic[T](collection, client, in, out, shape, settings, messageBinder)
}

private sealed trait SolrFlowState

private object SolrFlowStage {
  case object Idle extends SolrFlowState
  case object Sending extends SolrFlowState
  case object Finished extends SolrFlowState
}

sealed class SolrFlowLogic[T](
    collection: String,
    client: SolrClient,
    in: Inlet[IncomingMessage[T]],
    out: Outlet[Future[IncomingMessagesResult[T]]],
    shape: FlowShape[IncomingMessage[T], Future[IncomingMessagesResult[T]]],
    settings: SolrSinkSettings,
    messageBinder: T => SolrInputDocument)
    extends TimerGraphStageLogic(shape) with OutHandler with InHandler {

  private var state: SolrFlowState = Idle
  private val queue = new mutable.Queue[IncomingMessage[T]]()
  private var failedMessages: Seq[IncomingMessage[T]] = Nil
  private var retryCount: Int = 0

  override def onPull(): Unit =
    tryPull()

  override def onPush(): Unit = {
    queue.enqueue(grab(in))
    state match {
      case Idle => {
        state = Sending
        val messages = (1 to settings.bufferSize).flatMap { _ =>
          queue.dequeueFirst(_ => true)
        }
        sendBulkToSolr(messages)
      }
      case _ => ()
    }

    tryPull()
  }

  override def preStart(): Unit =
    pull(in)

  override def onTimer(timerKey: Any): Unit = {
    sendBulkToSolr(failedMessages)
    failedMessages = Nil
  }

  override def onUpstreamFailure(ex: Throwable): Unit =
    failStage(ex)

  override def onUpstreamFinish(): Unit = state match {
    case Idle     => handleSuccess()
    case Sending  => state = Finished
    case Finished => ()
  }

  private def tryPull(): Unit =
    if (queue.size < settings.bufferSize && !isClosed(in) && !hasBeenPulled(in)) {
      pull(in)
    }

  private def handleFailure(messages: Seq[IncomingMessage[T]], exc: Throwable): Unit = {
    if (retryCount >= settings.maxRetry && shouldRetry(exc)) {
      failStage(exc)
    } else {
      retryCount = retryCount + 1
      failedMessages = messages
      scheduleOnce(NotUsed, settings.retryInterval.millis)
    }
  }

  private def handleResponse(messages: Seq[IncomingMessage[T]], response: SolrResponseBase): Unit = {
    val result = IncomingMessagesResult(messages.map(_.source), response.getStatus)
    emit(out, Future.successful(result))

    val nextMessages = (1 to settings.bufferSize).flatMap { _ =>
      queue.dequeueFirst(_ => true)
    }

    if (nextMessages.isEmpty) {
      state match {
        case Finished => handleSuccess()
        case _        => state = Idle
      }
    } else {
      sendBulkToSolr(nextMessages)
    }
  }

  private def handleSuccess(): Unit =
    completeStage()

  private def sendBulkToSolr(messages: Seq[IncomingMessage[T]]): Unit = {
    val docs = messages.view.map(_.source).map(messageBinder)
    try {
      val response = client.add(collection, docs.asJava, settings.commitWithin)
      handleResponse(messages, response)
    } catch {
      case ex: Exception =>
        handleFailure(messages, ex)
    }
  }

  private def shouldRetry(exc: Throwable): Boolean = {
    val rootCause = SolrException.getRootCause(exc)
    rootCause match {
      case _: ConnectException        => true
      case _: NoHttpResponseException => true
      case _: SocketException         => true
      case _                          => false
    }
  }
}
