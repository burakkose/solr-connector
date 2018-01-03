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
   * Java API: creates a [[SolrFlowStage]] with [[DocumentObjectBinder]]
   */
  def create[T](collection: String,
                settings: SolrSinkSettings,
                client: SolrClient
  ): javadsl.Flow[IncomingMessage[T, NotUsed], JavaList[IncomingMessageResult[T, NotUsed]], NotUsed] =
    ScalaSolrFlow[T](collection, settings.asScala)(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] with custom binder
   */
  def create[T](collection: String,
                settings: SolrSinkSettings,
                binder: Function[T, SolrInputDocument],
                client: SolrClient
  ): javadsl.Flow[IncomingMessage[T, NotUsed], JavaList[IncomingMessageResult[T, NotUsed]], NotUsed] =
    ScalaSolrFlow[T](collection, settings.asScala, i => binder.apply(i))(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] with [[DocumentObjectBinder]] and passThrough
   */
  def withPassThrough[T, C](
      collection: String,
      settings: SolrSinkSettings,
      client: SolrClient
  ): javadsl.Flow[IncomingMessage[T, C], JavaList[IncomingMessageResult[T, C]], NotUsed] =
    ScalaSolrFlow.withPassThrough[T, C](collection, settings.asScala)(client)
      .map(_.asJava)
      .asJava

  /**
   * Java API: creates a [[SolrFlowStage]] with custom binder and passThrough
   */
  def withPassThrough[T, C](
      collection: String,
      settings: SolrSinkSettings,
      binder: Function[T, SolrInputDocument],
      client: SolrClient
  ): javadsl.Flow[IncomingMessage[T, C], JavaList[IncomingMessageResult[T, C]], NotUsed] =
    ScalaSolrFlow.withPassThrough[T, C](collection, settings.asScala, i => binder.apply(i))(client)
      .map(_.asJava)
      .asJava

}
