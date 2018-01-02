package akka.stream.burakkose.solr.scaladsl

import akka.Done
import akka.stream.burakkose.solr.IncomingMessage
import akka.stream.scaladsl.{Keep, Sink}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.common.SolrInputDocument

import scala.concurrent.Future

object SolrSink {
  def create[T](collection: String, settings: SolrSinkSettings)(
      implicit client: SolrClient,
      binder: Option[T => SolrInputDocument] = None
  ): Sink[IncomingMessage[T], Future[Done]] =
    SolrFlow
      .create[T](collection, settings)
      .toMat(Sink.ignore)(Keep.right)
}
