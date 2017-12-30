package akka.stream.burakkose.solr

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.apache.solr.client.solrj.io.Tuple
import org.apache.solr.client.solrj.io.stream.TupleStream

final class SolrSourceStage(collection: String, tupleStream: TupleStream)
    extends GraphStage[SourceShape[Tuple]] {

  val out: Outlet[Tuple] = Outlet("SolrSource.out")
  override val shape: SourceShape[Tuple] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes) =
    new SolrSourceLogic(collection, tupleStream, out, shape)
}

sealed class SolrSourceLogic(collection: String,
                             tupleStream: TupleStream,
                             out: Outlet[Tuple],
                             shape: SourceShape[Tuple])
    extends GraphStageLogic(shape)
    with OutHandler {

  override def preStart(): Unit =
    tupleStream.open()

  override def postStop(): Unit =
    tupleStream.close()

  override def onPull(): Unit = fetchFromSolr()

  private def fetchFromSolr(): Unit = {
    val tuple = tupleStream.read()
    if (tuple.EOF) {
      completeStage()
    } else if (tuple.EXCEPTION) {
      failStage(new IllegalStateException(tuple.getException))
    } else {
      emit(out, tuple)
    }
  }
}
