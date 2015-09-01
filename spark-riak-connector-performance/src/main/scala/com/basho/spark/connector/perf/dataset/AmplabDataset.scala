package com.basho.spark.connector.perf.dataset

import scala.collection.JavaConversions._
import java.io.InputStream
import org.apache.commons.lang3.StringUtils
import java.util.UUID

/**
 * @author anekhaev
 */
class AmplabDataset(
    val dataSize: String = "tiny",          // available options: 'tiny', '1node', '5nodes'
    val dataSetName: String = "uservisits"  // available options: 'rankings', 'uservisits', 'documents'
  ) {

  val testDataS3Bucket = "big-data-benchmark"

  def listDataPaths: List[(String, String)] = {
    S3Client
      .listChildrenKeys(testDataS3Bucket, s"pavlo/text/$dataSize/$dataSetName")
      .map(key => (testDataS3Bucket, key))
  }
   
  def extractRiakRowsToAdd(dataPath: (String, String)): Iterator[String] = {
    S3Client
      .loadTextFile(dataPath._1, dataPath._2)
      .map(dataLineToRiakJson)
  }
  
  def dataLineToRiakJson(row: String): String = {
    s"{ key: '${UUID.randomUUID()}', indexes: {testIndex: 1}, value: '${StringUtils.replace(row, "'", "\\'")}' }"
  }


}