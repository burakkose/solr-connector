package akka.stream.burakkose.solr

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.embedded.JettyConfig
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, UpdateRequest}
import org.apache.solr.cloud.{MiniSolrCloudCluster, ZkTestServer}
import org.junit.Assert.assertTrue
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

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
    val testWorkingDir = new File(targetDir, "scala-solr-" + System.currentTimeMillis)
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
      .createCollection("collection1", "conf", 2, 1)
      .process(client)
}
