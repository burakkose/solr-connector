package akka.stream.burakkose.solr.scaladsl

final case class SolrSinkSettings(bufferSize: Int = 10,
                                  retryInterval: Int = 5000,
                                  maxRetry: Int = 100,
                                  retryPartialFailure: Boolean = true,
                                  commitWithin: Int = -1)