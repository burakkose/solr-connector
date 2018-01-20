package akka.stream.burakkose.solr

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.apache.solr.client.solrj.io.Tuple
import org.apache.solr.client.solrj.io.stream.TupleStream

import scala.util.control.NonFatal

final class SolrSourceStage(
    collection: String,
    tupleStream: TupleStream
) extends GraphStage[SourceShape[Tuple]] {

  val out: Outlet[Tuple] = Outlet("SolrSource.out")
  override val shape: SourceShape[Tuple] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes) =
    new SolrSourceLogic(collection, tupleStream, out, shape)
}

final class SolrSourceLogic(
    collection: String,
    tupleStream: TupleStream,
    out: Outlet[Tuple],
    shape: SourceShape[Tuple]
) extends GraphStageLogic(shape)
    with OutHandler {

  setHandler(out, this)

  override def preStart(): Unit =
    try {
      tupleStream.open()
    } catch {
      case NonFatal(exc) =>
        failStage(exc)
    }

  override def postStop(): Unit =
    try {
      tupleStream.close()
    } catch {
      case NonFatal(exc) =>
        failStage(exc)
    }

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
