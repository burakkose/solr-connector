package akka.stream.burakkose.solr.scaladsl

import akka.NotUsed
import akka.stream.burakkose.solr.SolrSourceStage
import akka.stream.scaladsl.Source
import org.apache.solr.client.solrj.io.Tuple
import org.apache.solr.client.solrj.io.stream.TupleStream

object SolrSource {
  def create(collection: String, typeName: String)(
    implicit tupleStream: TupleStream): Source[Tuple, NotUsed] =
    Source.fromGraph(new SolrSourceStage(collection, tupleStream))
}
