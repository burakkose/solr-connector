package akka.stream.burakkose.solr;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.burakkose.solr.javadsl.SolrFlow;
import akka.stream.burakkose.solr.javadsl.SolrSink;
import akka.stream.burakkose.solr.javadsl.SolrSinkSettings;
import akka.stream.burakkose.solr.javadsl.SolrSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SolrTest {
    private static MiniSolrCloudCluster cluster;
    private static ActorSystem system;
    private static ActorMaterializer materializer;
    private static SolrClient client;
    private static String zkHost;

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

    @Test
    public void solrInputDocumentStream() throws Exception {
        //copy from collection1 to collection2
        createCollection("collection2"); //create a new collection

        StreamFactory factory = new StreamFactory()
                .withCollectionZkHost("collection1", zkHost);
        SolrClientCache solrClientCache = new SolrClientCache();
        StreamContext streamContext = new StreamContext();
        streamContext.setSolrClientCache(solrClientCache);

        StreamExpression expression1 = StreamExpressionParser.parse(
                "search(collection1, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream1 = new CloudSolrStream(expression1, factory);
        stream1.setStreamContext(streamContext);

        SolrSinkSettings sinkSettings = new SolrSinkSettings().withCommitWithin(5);
        CompletionStage<Done> res1 = SolrSource.create(
                "collection1",
                stream1
        ).map(tuple -> {
            Book book = tupleToBook.apply(tuple);
            SolrInputDocument doc = bookToDoc.apply(book);
            return IncomingMessage.create(doc);
        }).runWith(
                SolrSink.document(
                        "collection2",
                        sinkSettings,
                        client
                ),
                materializer
        );

        res1.toCompletableFuture().get();

        StreamFactory factory2 = new StreamFactory()
                .withCollectionZkHost("collection2", zkHost);

        StreamExpression expression2 = StreamExpressionParser.parse(
                "search(collection2, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream2 = new CloudSolrStream(expression2, factory2);
        stream2.setStreamContext(streamContext);

        CompletionStage<List<String>> res2 = SolrSource.create(
                "collection2",
                stream2
        )
                .map(t -> tupleToBook.apply(t).title)
                .runWith(Sink.seq(), materializer);

        List<String> result = new ArrayList<>(res2.toCompletableFuture().get());

        List<String> expect = Arrays.asList(
                "Akka Concurrency",
                "Akka in Action",
                "Effective Akka",
                "Learning Scala",
                "Programming in Scala",
                "Scala Puzzlers",
                "Scala for Spark in Production"
        );

        assertEquals(expect, result);
    }

    @Test
    public void beanStream() throws Exception {
        //copy from collection1 to collection3
        createCollection("collection3"); //create a new collection

        class BookBean {
            @Field("title")
            public String title;

            public BookBean(String title) {
                this.title = title;
            }
        }

        StreamFactory factory = new StreamFactory()
                .withCollectionZkHost("collection1", zkHost);
        SolrClientCache solrClientCache = new SolrClientCache();
        StreamContext streamContext = new StreamContext();
        streamContext.setSolrClientCache(solrClientCache);

        StreamExpression expression1 = StreamExpressionParser.parse(
                "search(collection1, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream1 = new CloudSolrStream(expression1, factory);
        stream1.setStreamContext(streamContext);

        SolrSinkSettings sinkSettings = new SolrSinkSettings().withCommitWithin(5);
        CompletionStage<Done> res1 = SolrSource.create(
                "collection1",
                stream1
        ).map(tuple -> {
            String title = tuple.getString("title");
            return IncomingMessage.create(new BookBean(title));
        }).runWith(
                SolrSink.bean(
                        "collection3",
                        sinkSettings,
                        client,
                        BookBean.class
                ),
                materializer
        );

        res1.toCompletableFuture().get();

        StreamFactory factory2 = new StreamFactory()
                .withCollectionZkHost("collection3", zkHost);

        StreamExpression expression2 = StreamExpressionParser.parse(
                "search(collection3, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream2 = new CloudSolrStream(expression2, factory2);
        stream2.setStreamContext(streamContext);

        CompletionStage<List<String>> res2 = SolrSource.create(
                "collection3",
                stream2
        )
                .map(t -> tupleToBook.apply(t).title)
                .runWith(Sink.seq(), materializer);

        List<String> result = new ArrayList<>(res2.toCompletableFuture().get());

        List<String> expect = Arrays.asList(
                "Akka Concurrency",
                "Akka in Action",
                "Effective Akka",
                "Learning Scala",
                "Programming in Scala",
                "Scala Puzzlers",
                "Scala for Spark in Production"
        );

        assertEquals(expect, result);
    }

    @Test
    public void typedStream() throws Exception {
        //copy from collection1 to collection2
        createCollection("collection4"); //create a new collection

        StreamFactory factory = new StreamFactory()
                .withCollectionZkHost("collection1", zkHost);
        SolrClientCache solrClientCache = new SolrClientCache();
        StreamContext streamContext = new StreamContext();
        streamContext.setSolrClientCache(solrClientCache);

        StreamExpression expression1 = StreamExpressionParser.parse(
                "search(collection1, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream1 = new CloudSolrStream(expression1, factory);
        stream1.setStreamContext(streamContext);

        SolrSinkSettings sinkSettings = new SolrSinkSettings().withCommitWithin(5);
        CompletionStage<Done> res1 = SolrSource.create(
                "collection1",
                stream1
        )
                .map(tuple -> IncomingMessage.create(tupleToBook.apply(tuple)))
                .runWith(
                        SolrSink.typed(
                                "collection4",
                                sinkSettings,
                                bookToDoc,
                                client,
                                Book.class
                        ),
                        materializer
                );

        res1.toCompletableFuture().get();

        StreamFactory factory2 = new StreamFactory()
                .withCollectionZkHost("collection4", zkHost);

        StreamExpression expression2 = StreamExpressionParser.parse(
                "search(collection4, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream2 = new CloudSolrStream(expression2, factory2);
        stream2.setStreamContext(streamContext);

        CompletionStage<List<String>> res2 = SolrSource.create(
                "collection4",
                stream2
        )
                .map(t -> tupleToBook.apply(t).title)
                .runWith(Sink.seq(), materializer);

        List<String> result = new ArrayList<>(res2.toCompletableFuture().get());

        List<String> expect = Arrays.asList(
                "Akka Concurrency",
                "Akka in Action",
                "Effective Akka",
                "Learning Scala",
                "Programming in Scala",
                "Scala Puzzlers",
                "Scala for Spark in Production"
        );

        assertEquals(expect, result);
    }

    @Test
    public void flow() throws Exception {
        // Copy collection1 to collection3
        createCollection("collection5"); //create a new collection

        StreamFactory factory = new StreamFactory()
                .withCollectionZkHost("collection1", zkHost);
        SolrClientCache solrClientCache = new SolrClientCache();
        StreamContext streamContext = new StreamContext();
        streamContext.setSolrClientCache(solrClientCache);

        StreamExpression expression1 = StreamExpressionParser.parse(
                "search(collection1, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream1 = new CloudSolrStream(expression1, factory);
        stream1.setStreamContext(streamContext);

        SolrSinkSettings sinkSettings = new SolrSinkSettings().withCommitWithin(5);
        CompletionStage<Done> res1 = SolrSource.create(
                "collection1",
                stream1
        )
                .map(tuple -> IncomingMessage.create(tupleToBook.apply(tuple)))
                .via(
                        SolrFlow.typed(
                                "collection5",
                                sinkSettings,
                                bookToDoc,
                                client,
                                Book.class
                        )
                )
                .runWith(Sink.ignore(), materializer);

        res1.toCompletableFuture().get();

        StreamFactory factory2 = new StreamFactory()
                .withCollectionZkHost("collection5", zkHost);

        StreamExpression expression2 = StreamExpressionParser.parse(
                "search(collection5, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream2 = new CloudSolrStream(expression2, factory2);
        stream2.setStreamContext(streamContext);

        CompletionStage<List<String>> res2 = SolrSource.create(
                "collection5",
                stream2
        )
                .map(t -> tupleToBook.apply(t).title)
                .runWith(Sink.seq(), materializer);

        List<String> result = new ArrayList<>(res2.toCompletableFuture().get());

        List<String> expect = Arrays.asList(
                "Akka Concurrency",
                "Akka in Action",
                "Effective Akka",
                "Learning Scala",
                "Programming in Scala",
                "Scala Puzzlers",
                "Scala for Spark in Production"
        );

        assertEquals(expect, result);
    }

    @Test
    public void testKafkaExample() throws Exception {
        //#kafka-example
        // We're going to pretend we got messages from kafka.
        // After we've written them to Solr, we want
        // to commit the offset to Kafka

        List<KafkaMessage> messagesFromKafka = Arrays.asList(
                new KafkaMessage(new Book("Book 1"), new KafkaOffset(0)),
                new KafkaMessage(new Book("Book 2"), new KafkaOffset(1)),
                new KafkaMessage(new Book("Book 3"), new KafkaOffset(2))
        );

        final KafkaCommitter kafkaCommitter = new KafkaCommitter();

        createCollection("collection6"); //create new collection
        SolrSinkSettings sinkSettings = new SolrSinkSettings().withCommitWithin(5);

        Source.from(messagesFromKafka) // Assume we get this from Kafka
                .map(kafkaMessage -> {
                    Book book = kafkaMessage.book;
                    // Transform message so that we can write to elastic
                    return IncomingMessage.create(book, kafkaMessage.offset);
                })
                .via(
                        SolrFlow.typedWithPassThrough(
                                "collection6",
                                sinkSettings,
                                bookToDoc,
                                client,
                                Book.class
                        )
                ).map(messageResults -> {
            messageResults.stream()
                    .forEach(result -> {
                        if (result.status() != 0) {
                            throw new RuntimeException("Failed to write message to elastic");
                        }
                        // Commit to kafka
                        kafkaCommitter.commit(result.passThrough());
                    });
            return NotUsed.getInstance();
        }).runWith(Sink.seq(), materializer) // Run it
                .toCompletableFuture().get(); // Wait for it to complete

        // Make sure all messages was committed to kafka
        assertEquals(Arrays.asList(0, 1, 2), kafkaCommitter.committedOffsets);

        StreamFactory factory = new StreamFactory()
                .withCollectionZkHost("collection6", zkHost);

        StreamExpression expression = StreamExpressionParser.parse(
                "search(collection6, q=*:*, fl=\"title\", sort=\"title asc\")");
        TupleStream stream = new CloudSolrStream(expression, factory);
        SolrClientCache solrClientCache = new SolrClientCache();
        StreamContext streamContext = new StreamContext();
        streamContext.setSolrClientCache(solrClientCache);
        stream.setStreamContext(streamContext);

        CompletionStage<List<String>> res2 = SolrSource.create(
                "collection6",
                stream
        )
                .map(t -> tupleToBook.apply(t).title)
                .runWith(Sink.seq(), materializer);

        List<String> result = new ArrayList<>(res2.toCompletableFuture().get());

        assertEquals(
                messagesFromKafka.stream().map(m -> m.book.title).sorted().collect(Collectors.toList()),
                result.stream().sorted().collect(Collectors.toList())
        );
    }

    @BeforeClass
    public static void setup() throws Exception {
        setupCluster();

        //#init-client
        zkHost = "127.0.0.1:9984/solr";
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

    static class KafkaCommitter {
        List<Integer> committedOffsets = new ArrayList<>();

        public KafkaCommitter() {
        }

        void commit(KafkaOffset offset) {
            committedOffsets.add(offset.offset);
        }
    }

    static class KafkaOffset {
        final int offset;

        public KafkaOffset(int offset) {
            this.offset = offset;
        }

    }

    static class KafkaMessage {
        final Book book;
        final KafkaOffset offset;

        public KafkaMessage(Book book, KafkaOffset offset) {
            this.book = book;
            this.offset = offset;
        }

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
        ZkTestServer zkTestServer = new ZkTestServer(zkDir, 9984);
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

        assertTrue(!cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes().isEmpty());
    }

    private static void createCollection(String name) throws IOException, SolrServerException {
        CollectionAdminRequest
                .createCollection(name, "conf", 2, 1)
                .process(cluster.getSolrClient());
    }
}