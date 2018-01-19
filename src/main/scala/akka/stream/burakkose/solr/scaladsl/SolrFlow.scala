package akka.stream.burakkose.solr.scaladsl

import akka.NotUsed
import akka.stream.burakkose.solr.{IncomingMessage, IncomingMessageResult, SolrFlowStage}
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.beans.DocumentObjectBinder
import org.apache.solr.common.SolrInputDocument

object SolrFlow {

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]].
   */
  def document(
      collection: String,
      settings: SolrSinkSettings
  )(implicit client: SolrClient): Flow[IncomingMessage[SolrInputDocument, NotUsed], Seq[
    IncomingMessageResult[SolrInputDocument, NotUsed]
  ], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[SolrInputDocument, NotUsed](
          collection,
          client,
          settings,
          identity
        )
      )
      .mapAsync(1)(identity)

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with [[DocumentObjectBinder]].
   */
  def bean[T](
      collection: String,
      settings: SolrSinkSettings
  )(implicit client: SolrClient): Flow[IncomingMessage[T, NotUsed], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, NotUsed](
          collection,
          client,
          settings,
          new DefaultSolrObjectBinder
        )
      )
      .mapAsync(1)(identity)

  /**
   * Scala API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with `binder` of type 'T'.
   */
  def typed[T](
      collection: String,
      settings: SolrSinkSettings,
      binder: T => SolrInputDocument
  )(implicit client: SolrClient): Flow[IncomingMessage[T, NotUsed], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, NotUsed](
          collection,
          client,
          settings,
          binder
        )
      )
      .mapAsync(1)(identity)

  /**
   * Scala API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C`.
   */
  def documentWithPassThrough[C](
      collection: String,
      settings: SolrSinkSettings
  )(
      implicit client: SolrClient
  ): Flow[IncomingMessage[SolrInputDocument, C], Seq[IncomingMessageResult[SolrInputDocument, C]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[SolrInputDocument, C](
          collection,
          client,
          settings,
          identity
        )
      )
      .mapAsync(1)(identity)

  /**
   * Scala API: creates a [[SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C` and [[DocumentObjectBinder]] for type 'T' .
   */
  def beanWithPassThrough[T, C](
      collection: String,
      settings: SolrSinkSettings
  )(implicit client: SolrClient): Flow[IncomingMessage[T, C], Seq[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, C](
          collection,
          client,
          settings,
          new DefaultSolrObjectBinder
        )
      )
      .mapAsync(1)(identity)

  /**
   * Scala API: creates a [[SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C` and `binder` of type `T`.
   */
  def typedWithPassThrough[T, C](
      collection: String,
      settings: SolrSinkSettings,
      binder: T => SolrInputDocument
  )(implicit client: SolrClient): Flow[IncomingMessage[T, C], Seq[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, C](
          collection,
          client,
          settings,
          binder
        )
      )
      .mapAsync(1)(identity)

  private class DefaultSolrObjectBinder(implicit c: SolrClient) extends (Any => SolrInputDocument) {
    override def apply(v1: Any): SolrInputDocument =
      c.getBinder.toSolrInputDocument(v1)
  }

}
