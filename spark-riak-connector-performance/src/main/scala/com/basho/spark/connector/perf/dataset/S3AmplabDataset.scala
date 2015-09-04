package com.basho.spark.connector.perf.dataset

import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * @author anekhaev
 */
class S3AmplabDataset(s3Bucket: String, s3Path: String, fileLimit: Option[Int]) extends AmplabDataset[(String, String)] with LazyLogging {
  
  def listDataPaths: List[(String, String)] = {
    logger.debug(s"Grabbing dataset file paths from s3://$s3Bucket/$s3Path*...")
    val paths = S3Client
      .listChildrenKeys(s3Bucket, s3Path)
      .map(key => (s3Bucket, key))
    logger.info(s"Discovered ${paths.size} files in the dataset, the limit is: $fileLimit")
    fileLimit.map(paths.take(_)).getOrElse(paths)
  }
   
  def extractRiakRowsToAdd(dataPath: (String, String)): Iterator[String] = {
    S3Client
      .loadTextFile(dataPath._1, dataPath._2)
      .map(dataLineToRiakJson)
  }
  
}