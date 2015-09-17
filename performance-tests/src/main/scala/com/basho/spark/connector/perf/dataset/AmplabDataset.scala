package com.basho.spark.connector.perf.dataset

import java.util.UUID

import org.apache.commons.lang3.StringUtils

/**
 * @author anekhaev
 */
trait AmplabDataset[T] {


  def listDataPaths: List[T]
   
  def extractRiakRowsToAdd(dataPath: T): Iterator[String]
  
  def dataLineToRiakJson(row: String): String = {
    s"{ key: '${UUID.randomUUID()}', indexes: {testIndex: 1}, value: '${StringUtils.replace(row, "'", "\\'")}' }"
  }


}