package akka.stream.burakkose.solr.scaladsl

import akka.{Done, NotUsed}
import akka.stream.burakkose.solr.IncomingMessage
import akka.stream.scaladsl.{Keep, Sink}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.beans.DocumentObjectBinder
import org.apache.solr.common.SolrInputDocument

import scala.concurrent.Future

object SolrSink {

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing [[SolrInputDocument]].
   */
  def document[T](collection: String, settings: SolrSinkSettings)(
      implicit client: SolrClient
  ): Sink[IncomingMessage[SolrInputDocument, NotUsed], Future[Done]] =
    SolrFlow
      .document(collection, settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T` with [[DocumentObjectBinder]].
   */
  def bean[T](collection: String, settings: SolrSinkSettings)(
      implicit client: SolrClient
  ): Sink[IncomingMessage[T, NotUsed], Future[Done]] =
    SolrFlow
      .bean[T](collection, settings)
      .toMat(Sink.ignore)(Keep.right)

  /**
   * Scala API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T` with `binder` of type 'T'.
    */
  def typed[T](
      collection: String,
      settings: SolrSinkSettings,
      binder: T => SolrInputDocument
  )(implicit client: SolrClient)
    : Sink[IncomingMessage[T, NotUsed], Future[Done]] =
    SolrFlow
      .typed[T](collection, settings, binder)
      .toMat(Sink.ignore)(Keep.right)
}
