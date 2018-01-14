package akka.stream.burakkose.solr

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.burakkose.solr.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.embedded.JettyConfig
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider
import org.apache.solr.client.solrj.io.stream.expr.{StreamExpressionParser, StreamFactory}
import org.apache.solr.client.solrj.io.stream.{CloudSolrStream, StreamContext, TupleStream}
import org.apache.solr.client.solrj.io.{SolrClientCache, Tuple}
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, UpdateRequest}
import org.apache.solr.cloud.{MiniSolrCloudCluster, ZkTestServer}
import org.apache.solr.common.SolrInputDocument
import org.junit.Assert.assertTrue
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SolrSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  private var cluster: MiniSolrCloudCluster = _

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  //#init-mat
  //#init-client
  import org.apache.solr.client.solrj.impl.CloudSolrClient

  implicit val client: SolrClient =
    new CloudSolrClient.Builder().withZkHost("127.0.0.1:9983/solr").build
  //#init-client
  //#define-class
  case class Book(title: String)

  def fromBookToDoc(b: Book): SolrInputDocument = {
    val doc = new SolrInputDocument
    doc.setField("title", b.title)
    doc
  }

  def fromTupleToBook(tup: Tuple): Book = {
    val title = tup.getString("title")
    Book(title)
  }
  //#define-class

  "Solr connector" should {
    "consume and publish document" in {
      //copy from collection1 to collection2
      createCollection("collection2") //create a new collection

      val factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer.getZkAddress)
      val solrClientCache = new SolrClientCache()
      val streamContext = new StreamContext()
      streamContext.setSolrClientCache(solrClientCache)

      val expression1 = StreamExpressionParser.parse(
        """search(collection1, q=*:*, fl="title", sort="title asc")""")
      val stream1: TupleStream = new CloudSolrStream(expression1, factory)
      stream1.setStreamContext(streamContext)

      val res1 = SolrSource
        .create(
          collection = "collection1",
          tupleStream = stream1
        )
        .map { tuple: Tuple =>
          val book: Book = fromTupleToBook(tuple)
          IncomingMessage(book)
        }
        .runWith(
          SolrSink
            .create[Book](
              collection = "collection2",
              settings = SolrSinkSettings(commitWithin = 1),
              binder = fromBookToDoc(_)
            )
        )

      Await.result(res1, Duration.Inf)

      val factory2 = new StreamFactory()
        .withCollectionZkHost("collection2", "127.0.0.1:9983/solr")

      val expression2 = StreamExpressionParser.parse(
        """search(collection2, q=*:*, fl="title", sort="title asc")""")
      val stream2: TupleStream = new CloudSolrStream(expression2, factory2)
      stream2.setStreamContext(streamContext)

      val res2 = SolrSource
        .create("collection2", tupleStream = stream2)
        .map(fromTupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

  "SolrFlow" should {
    "store documents and pass with status to downstream" in {
      // Copy collection1 to collection3
      createCollection("collection3") //create a new collection

      val factory = new StreamFactory()
        .withCollectionZkHost("collection1", cluster.getZkServer.getZkAddress)
      val solrClientCache = new SolrClientCache()
      val streamContext = new StreamContext()
      streamContext.setSolrClientCache(solrClientCache)

      val expression1 = StreamExpressionParser.parse(
        """search(collection1, q=*:*, fl="title", sort="title asc")""")
      val stream1: TupleStream = new CloudSolrStream(expression1, factory)
      stream1.setStreamContext(streamContext)

      val res1 = SolrSource
        .create(collection = "collection1", tupleStream = stream1)
        .map { tuple: Tuple =>
          val book: Book = fromTupleToBook(tuple)
          IncomingMessage(book)
        }
        .via(
          SolrFlow
            .create[Book](
              collection = "collection3",
              settings = SolrSinkSettings(commitWithin = 1),
              binder = fromBookToDoc(_)
            )
        )
        .runWith(Sink.seq)

      val result1 = Await.result(res1, Duration.Inf)

      // Assert no errors
      assert(result1.forall(_.exists(_.status == 0)))

      val factory2 = new StreamFactory()
        .withCollectionZkHost("collection3", "127.0.0.1:9983/solr")

      val expression2 = StreamExpressionParser.parse(
        """search(collection3, q=*:*, fl="title", sort="title asc")""")
      val stream2: TupleStream = new CloudSolrStream(expression2, factory2)
      stream2.setStreamContext(streamContext)

      val res2 = SolrSource
        .create("collection3", tupleStream = stream2)
        .map(fromTupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

  "SolrFlow" should {
    "kafka-example - store documents and pass Responses with passThrough" in {
      //#kafka-example
      // We're going to pretend we got messages from kafka.
      // After we've written them to Solr, we want
      // to commit the offset to Kafka

      case class KafkaOffset(offset: Int)
      case class KafkaMessage(book: Book, offset: KafkaOffset)

      val messagesFromKafka = List(
        KafkaMessage(Book("Book 1"), KafkaOffset(0)),
        KafkaMessage(Book("Book 2"), KafkaOffset(1)),
        KafkaMessage(Book("Book 3"), KafkaOffset(2))
      )

      var committedOffsets = List[KafkaOffset]()

      def commitToKakfa(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      createCollection("collection4") //create new collection

      val res1 = Source(messagesFromKafka)
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          val id = book.title
          println("title: " + book.title)

          // Transform message so that we can write to solr
          IncomingMessage(book, kafkaMessage.offset)
        }
        .via( // write to Solr
          SolrFlow.withPassThrough[Book, KafkaOffset](
            collection = "collection4",
            settings = SolrSinkSettings(commitWithin = 5),
            binder = fromBookToDoc(_)
          )
        )
        .map { messageResults =>
          messageResults.foreach { result =>
            if (result.status != 0)
              throw new Exception("Failed to write message to Solr")
            // Commit to kafka
            commitToKakfa(result.passThrough)
          }
        }
        .runWith(Sink.ignore)

      Await.ready(res1, Duration.Inf)

      // Make sure all messages was committed to kafka
      assert(List(0, 1, 2) == committedOffsets.map(_.offset))

      val factory = new StreamFactory()
        .withCollectionZkHost("collection4", cluster.getZkServer.getZkAddress)
      val solrClientCache = new SolrClientCache()
      val streamContext = new StreamContext()
      streamContext.setSolrClientCache(solrClientCache)

      val expression = StreamExpressionParser.parse(
        """search(collection4, q=*:*, fl="title", sort="title asc")""")
      val stream: TupleStream = new CloudSolrStream(expression, factory)
      stream.setStreamContext(streamContext)

      val res2 = SolrSource.create("collection4", stream)
        .map(fromTupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf).toList

      result.sorted shouldEqual messagesFromKafka.map(_.book.title).sorted
    }
  }

  override def beforeAll(): Unit = {
    setupCluster()
    new UpdateRequest()
      .add("title", "Akka in Action")
      .add("title", "Programming in Scala")
      .add("title", "Learning Scala")
      .add("title", "Scala for Spark in Production")
      .add("title", "Scala Puzzlers")
      .add("title", "Effective Akka")
      .add("title", "Akka Concurrency")
      .commit(client, "collection1")
  }

  override def afterAll(): Unit = {
    client.close()
    cluster.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  private def setupCluster(): Unit = {
    val targetDir = new File("target")
    val testWorkingDir =
      new File(targetDir, "scala-solr-" + System.currentTimeMillis)
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs

    val confDir = new File("src/test/resources/conf")

    val zkDir = testWorkingDir.toPath.resolve("zookeeper/server/data").toString
    val zkTestServer = new ZkTestServer(zkDir, 9983)
    zkTestServer.run()

    cluster = new MiniSolrCloudCluster(
      4,
      testWorkingDir.toPath,
      MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML,
      JettyConfig.builder.setContext("/solr").build,
      zkTestServer
    )
    cluster.getSolrClient.getClusterStateProvider
      .asInstanceOf[ZkClientClusterStateProvider]
      .uploadConfig(confDir.toPath, "conf")

    createCollection("collection1")

    assertTrue(
      !cluster.getSolrClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
  }

  private def createCollection(name: String) =
    CollectionAdminRequest
      .createCollection(name, "conf", 2, 1)
      .process(client)
}
