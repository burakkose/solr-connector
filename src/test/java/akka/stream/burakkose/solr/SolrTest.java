package akka.stream.burakkose.solr;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;

public class SolrTest {
    private static MiniSolrCloudCluster cluster;
    private static ActorSystem system;
    private static ActorMaterializer materializer;
    private static SolrClient client;

    //#define-class
    public static class Book {
        public String title;

        public Book() {
        }

        public Book(String title) {
            this.title = title;
        }
    }

    Function<Book, SolrInputDocument> bookToDoc = book -> {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("title", book.title);
        return doc;
    };

    Function<Tuple, Book> tupleToBook = tuple -> {
        String title = tuple.getString("title");
        return new Book(title);
    };
    //#define-class

    @BeforeClass
    public static void setup() throws Exception {
        setupCluster();

        //#init-client
        final String zkHost = "127.0.0.1:9983/solr";
        client = new CloudSolrClient.Builder().withZkHost(zkHost).build();
        //#init-client

        //#init-mat
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#init-mat

        new UpdateRequest()
                .add("title", "Akka in Action")
                .add("title", "Programming in Scala")
                .add("title", "Learning Scala")
                .add("title", "Scala for Spark in Production")
                .add("title", "Scala Puzzlers")
                .add("title", "Effective Akka")
                .add("title", "Akka Concurrency")
                .commit(client, "collection1");
    }


    private static void setupCluster() throws Exception {
        File targetDir = new File("target");
        File testWorkingDir =
                new File(targetDir, "java-solr-" + System.currentTimeMillis());
        if (!testWorkingDir.isDirectory()) {
            boolean mkdirs = testWorkingDir.mkdirs();
        }

        File confDir = new File("src/test/resources/conf");

        String zkDir = testWorkingDir.toPath().resolve("zookeeper/server/data").toString();
        ZkTestServer zkTestServer = new ZkTestServer(zkDir, 9983);
        zkTestServer.run();

        cluster = new MiniSolrCloudCluster(
                4,
                testWorkingDir.toPath(),
                MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML,
                JettyConfig.builder().setContext("/solr").build(),
                zkTestServer
        );
        ((ZkClientClusterStateProvider) cluster.getSolrClient().getClusterStateProvider())
                .uploadConfig(confDir.toPath(), "conf");

        createCollection("collection1");

        assertTrue(
                !cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes().isEmpty());
    }

    private static void createCollection(String name) throws IOException, SolrServerException {
        CollectionAdminRequest
                .createCollection(name, "conf", 2, 1)
                .process(client);
    }
}
