package akka.stream.burakkose.solr.scaladsl

import akka.NotUsed
import akka.stream.burakkose.solr.SolrSourceStage
import akka.stream.scaladsl.Source
import org.apache.solr.client.solrj.io.Tuple
import org.apache.solr.client.solrj.io.stream.TupleStream

object SolrSource {

  /**
   * Scala API: creates a [[SolrSourceStage]] that consumes as [[Tuple]]
   */
  def apply(collection: String, tupleStream: TupleStream): Source[Tuple, NotUsed] =
    Source.fromGraph(new SolrSourceStage(collection, tupleStream))
}
