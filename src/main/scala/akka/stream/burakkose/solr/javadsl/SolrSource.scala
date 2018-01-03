package akka.stream.burakkose.solr.javadsl

import akka.NotUsed
import akka.stream.burakkose.solr.SolrSourceStage
import akka.stream.javadsl.Source
import org.apache.solr.client.solrj.io.Tuple
import org.apache.solr.client.solrj.io.stream.TupleStream

object SolrSource {

  /**
   * Java API: creates a [[SolrSourceStage]] that consumes as [[Tuple]]
   */
  def create(collection: String, tupleStream: TupleStream): Source[Tuple, NotUsed] =
    Source.fromGraph(new SolrSourceStage(collection, tupleStream))
}
