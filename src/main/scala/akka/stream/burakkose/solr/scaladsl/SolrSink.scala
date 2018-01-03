package akka.stream.burakkose.solr.scaladsl

import akka.Done
import akka.stream.burakkose.solr.IncomingMessage
import akka.stream.scaladsl.{Keep, Sink}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.beans.DocumentObjectBinder
import org.apache.solr.common.SolrInputDocument

import scala.concurrent.Future

object SolrSink {

  /**
   * Scala API: creates a [[SolrFlow]] with [[DocumentObjectBinder]]
   */
  def apply[T](collection: String, settings: SolrSinkSettings)(
      implicit client: SolrClient
  ): Sink[IncomingMessage[T], Future[Done]] =
    SolrFlow[T](collection, settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow]] with custom binder
   */
  def apply[T](collection: String, settings: SolrSinkSettings, binder: T => SolrInputDocument)(
      implicit client: SolrClient,
  ): Sink[IncomingMessage[T], Future[Done]] =
    SolrFlow[T](collection, settings, binder)
      .toMat(Sink.ignore)(Keep.right)
}
