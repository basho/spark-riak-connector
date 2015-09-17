package com.basho.spark.connector.perf.dataset

import java.io.File
import scala.io.Source

/**
 * @author anekhaev
 */
class FileAmplabDataset(filePrefix: String, filesCount: Int) extends AmplabDataset[File] {
    
  def listDataPaths: List[File] = {
    (0 until filesCount).map(i => new File(filePrefix + i)).toList
  }

  def extractRiakRowsToAdd(dataPath: File): Iterator[String] = {
    Source.fromFile(dataPath)
      .getLines()
      .map(dataLineToRiakJson)
  }
  
}