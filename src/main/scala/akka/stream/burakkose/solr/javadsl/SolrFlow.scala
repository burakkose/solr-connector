package akka.stream.burakkose.solr.javadsl

import java.util.function.Function
import java.util.{List => JavaList}

import akka.NotUsed
import akka.stream.burakkose.solr.scaladsl.{SolrFlow => ScalaSolrFlow}
import akka.stream.burakkose.solr.{IncomingMessage, IncomingMessageResult, SolrFlowStage}
import akka.stream.javadsl
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.beans.DocumentObjectBinder
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._

object SolrFlow {

  /**
   * Java API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]].
   */
  def document(
      collection: String,
      settings: SolrSinkSettings,
      client: SolrClient
  ): javadsl.Flow[IncomingMessage[SolrInputDocument, NotUsed],
                  JavaList[IncomingMessageResult[SolrInputDocument, NotUsed]],
                  NotUsed] =
    ScalaSolrFlow
      .document(collection, settings.asScala)(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with [[DocumentObjectBinder]].
   */
  def bean[T](
      collection: String,
      settings: SolrSinkSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[IncomingMessage[T, NotUsed],
                  JavaList[IncomingMessageResult[T, NotUsed]],
                  NotUsed] =
    ScalaSolrFlow
      .bean[T](collection, settings.asScala)(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type `T` from [[IncomingMessage]] to sequences
   * of [[IncomingMessageResult]] with `binder` of type 'T'.
   */
  def typed[T](
      collection: String,
      settings: SolrSinkSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[IncomingMessage[T, NotUsed],
                  JavaList[IncomingMessageResult[T, NotUsed]],
                  NotUsed] =
    ScalaSolrFlow
      .typed[T](collection, settings.asScala, i => binder.apply(i))(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for [[SolrInputDocument]] from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C`.
   */
  def documentWithPassThrough[C](
      collection: String,
      settings: SolrSinkSettings,
      client: SolrClient
  ): javadsl.Flow[IncomingMessage[SolrInputDocument, C],
                  JavaList[IncomingMessageResult[SolrInputDocument, C]],
                  NotUsed] =
    ScalaSolrFlow
      .documentWithPassThrough[C](collection, settings.asScala)(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C` and [[DocumentObjectBinder]] for type 'T' .
   */
  def beanWithPassThrough[T, C](
      collection: String,
      settings: SolrSinkSettings,
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[IncomingMessage[T, C],
                  JavaList[IncomingMessageResult[T, C]],
                  NotUsed] =
    ScalaSolrFlow
      .beanWithPassThrough[T, C](collection, settings.asScala)(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] for type 'T' from [[IncomingMessage]]
   * to lists of [[IncomingMessageResult]] with `passThrough` of type `C` and `binder` of type `T`.
   */
  def typedWithPassThrough[T, C](
      collection: String,
      settings: SolrSinkSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient,
      clazz: Class[T]
  ): javadsl.Flow[IncomingMessage[T, C],
                  JavaList[IncomingMessageResult[T, C]],
                  NotUsed] =
    ScalaSolrFlow
      .typedWithPassThrough[T, C](collection,
                                  settings.asScala,
                                  i => binder.apply(i))(client)
      .map(_.asJava)
      .asJava

}
