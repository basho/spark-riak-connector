package com.basho.spark.connector.perf.riak

import com.basho.spark.connector.perf.dataset.S3Client
import com.basho.spark.connector.rdd.RiakFunctions
import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.perf.dataset.AmplabDataset
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.client.core.RiakNode
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * @author anekhaev
 */
class RiakClient(
    val riakHost: String,
    val riakPort: Int,
    override val numberOfParallelRequests: Int = 4) extends RiakFunctions {
  
  logger.info(s"Creating Riak client on $riakHost:$riakPort...")
 
  protected override val riakHosts: Set[HostAndPort] = 
    Set(HostAndPort.fromParts(riakHost, riakPort))
  
  protected override val nodeBuilder: RiakNode.Builder =
    new RiakNode.Builder()
      .withMinConnections(numberOfParallelRequests)
      .withRemoteAddress(riakHost)
      .withRemotePort(riakPort)

      
  def resetAndLoadDataset[T](namespace: Namespace, dataset: AmplabDataset[T]) = {
    logger.info(s"Resetting Riak bucket $namespace...")
    resetAndEmptyBucket(namespace)
    logger.info(s"Resetting of Riak bucket $namespace completed")

    val filePaths = dataset.listDataPaths
    
    logger.info(s"Loading dataset to Riak bucket $namespace...")
    withRiakDo { session =>
      filePaths foreach { path =>
        val riakRows = dataset.extractRiakRowsToAdd(path)
        val jsonData = riakRows.mkString("[", ",\n", "]")
        createValues(session, namespace, jsonData)
      }
    }
    logger.info(s"Loading dataset to Riak bucket $namespace completed")
  }

}