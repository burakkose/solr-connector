package akka.stream.burakkose.solr.scaladsl

import akka.NotUsed
import akka.stream.burakkose.solr.{IncomingMessage, IncomingMessagesResult, SolrFlowStage}
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.beans.DocumentObjectBinder
import org.apache.solr.common.SolrInputDocument

object SolrFlow {

  /**
   * Scala API: creates a [[SolrFlowStage]] with [[DocumentObjectBinder]]
   */
  def apply[T](collection: String, settings: SolrSinkSettings)(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T], IncomingMessagesResult[T], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T](
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
  def apply[T](collection: String, settings: SolrSinkSettings, binder: T => SolrInputDocument)(
      implicit client: SolrClient
  ): Flow[IncomingMessage[T], IncomingMessagesResult[T], NotUsed] =
    Flow
      .fromGraph(
        new SolrFlowStage[T](
          collection,
          client,
          settings,
          binder
        )
      )
      .mapAsync(1)(identity)

}
