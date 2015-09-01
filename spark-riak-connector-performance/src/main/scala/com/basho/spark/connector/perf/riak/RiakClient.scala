package com.basho.spark.connector.perf.riak

import com.basho.spark.connector.perf.dataset.S3Client
import com.basho.spark.connector.rdd.RiakFunctions
import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.perf.dataset.AmplabDataset
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.client.core.RiakNode

/**
 * @author anekhaev
 */
class RiakClient(
    val riakHost: String,
    val riakPort: Int,
    override val numberOfParallelRequests: Int = 4) extends RiakFunctions {
 
  protected override val riakHosts: Set[HostAndPort] = 
    Set(HostAndPort.fromParts(riakHost, riakPort))
  
  protected override val nodeBuilder: RiakNode.Builder =
    new RiakNode.Builder()
      .withMinConnections(numberOfParallelRequests)
      .withRemoteAddress(riakHost)
      .withRemotePort(riakPort)

      
  def resetAndLoadDataset(namespace: Namespace, dataset: AmplabDataset) = {
    resetAndEmptyBucket(namespace)

    val filePaths = dataset.listDataPaths.take(5)

    withRiakDo { session =>
      filePaths foreach { path =>
        val riakRows = dataset.extractRiakRowsToAdd(path)
        val jsonData = riakRows.mkString("[", ",\n", "]")
        createValues(session, namespace, jsonData)
      }
    }

  }

}