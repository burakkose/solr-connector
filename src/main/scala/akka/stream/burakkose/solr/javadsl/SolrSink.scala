package akka.stream.burakkose.solr.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.Function

import akka.stream.burakkose.solr.IncomingMessage
import akka.stream.javadsl
import akka.stream.javadsl.Sink
import akka.{Done, NotUsed}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.beans.DocumentObjectBinder
import org.apache.solr.common.SolrInputDocument

object SolrSink {

  /**
   * Java API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing [[SolrInputDocument]].
   */
  def document(
      collection: String,
      settings: SolrSinkSettings,
      client: SolrClient
  ): javadsl.Sink[IncomingMessage[SolrInputDocument, NotUsed],
                  CompletionStage[Done]] =
    SolrFlow
      .document(collection, settings, client)
      .toMat(javadsl.Sink.ignore,
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T` with [[DocumentObjectBinder]].
   */
  def bean[T](
      collection: String,
      settings: SolrSinkSettings,
      client: SolrClient
  ): Sink[IncomingMessage[T, NotUsed], CompletionStage[Done]] =
    SolrFlow
      .bean[T](collection, settings, client)
      .toMat(javadsl.Sink.ignore,
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])

  /**
   * Java API: creates a [[SolrFlow] to Solr for [[IncomingMessage]] containing type `T` with `binder` of type 'T'.
   */
  def typed[T](
      collection: String,
      settings: SolrSinkSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient
  ): javadsl.Sink[IncomingMessage[T, NotUsed], CompletionStage[Done]] =
    SolrFlow
      .typed[T](collection, settings, binder, client)
      .toMat(javadsl.Sink.ignore,
             javadsl.Keep.right[NotUsed, CompletionStage[Done]])
}