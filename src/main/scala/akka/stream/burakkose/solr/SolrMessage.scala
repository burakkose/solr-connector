package akka.stream.burakkose.solr

sealed trait SolrMessage

object SolrMessage {
  final case class OutgoingMessage[T](id: String, payload: T) extends SolrMessage
  final case class IncomingMessage[T](id: Option[String], payload: T) extends SolrMessage

  object IncomingMessage {
    def apply[T](id: String, payload: T): IncomingMessage[T] = new IncomingMessage(Option(id), payload)
  }
}