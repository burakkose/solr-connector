package akka.stream.burakkose.solr.scaladsl

import akka.NotUsed
import akka.stream.burakkose.solr.{
  IncomingMessage,
  IncomingMessageResult,
  SolrFlowStage
}
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.beans.DocumentObjectBinder
import org.apache.solr.common.SolrInputDocument

object SolrFlow {

  /**
   * Scala API: creates a [[SolrFlowStage]] with [[DocumentObjectBinder]]
   */
  def create[T](collection: String, settings: SolrSinkSettings)(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T, NotUsed], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, NotUsed](
          collection,
          client,
          settings,
          client.getBinder.toSolrInputDocument _
        )
      )
      .mapAsync(1)(identity)

  /**
   * Scala API: creates a [[SolrFlowStage]] with custom binder
   */
  def create[T](collection: String,
                settings: SolrSinkSettings,
                binder: T => SolrInputDocument)(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T, NotUsed], Seq[IncomingMessageResult[T, NotUsed]], NotUsed] =
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
   * Scala API: creates a [[SolrFlowStage]] with [[DocumentObjectBinder]] and passThrough
   */
  def withPassThrough[T, C](collection: String, settings: SolrSinkSettings)(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T, C], Seq[IncomingMessageResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T, C](
          collection,
          client,
          settings,
          client.getBinder.toSolrInputDocument _
        )
      )
      .mapAsync(1)(identity)

  /**
   * Scala API: creates a [[SolrFlowStage]] with custom binder and passThrough
   */
  def withPassThrough[T, C](collection: String,
                            settings: SolrSinkSettings,
                            binder: T => SolrInputDocument)(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T, C], Seq[IncomingMessageResult[T, C]], NotUsed] =
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

}
