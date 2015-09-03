package com.basho.spark.connector.perf.dataset

/**
 * @author anekhaev
 */
class S3AmplabDataset(s3Bucket: String, s3Path: String) extends AmplabDataset[(String, String)] {
  
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
  
}