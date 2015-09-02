package com.basho.spark.connector.perf.dataset

import scala.collection.JavaConversions._
import java.io.InputStream
import org.apache.commons.lang3.StringUtils
import java.util.UUID

/**
 * @author anekhaev
 */
class AmplabDataset(s3Bucket: String, s3Path: String) {


  def listDataPaths: List[(String, String)] = {
    S3Client
      .listChildrenKeys(s3Bucket, s3Path)
      .map(key => (s3Bucket, key))
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