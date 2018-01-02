package akka.stream.burakkose.solr.scaladsl

import akka.NotUsed
import akka.stream.burakkose.solr.{
  IncomingMessage,
  IncomingMessagesResult,
  SolrFlowStage
}
import akka.stream.scaladsl.Flow
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

object SolrFlow {
  def create[T](collection: String, settings: SolrSinkSettings)(
      implicit client: SolrClient,
      binder: Option[T => SolrInputDocument] = None
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
