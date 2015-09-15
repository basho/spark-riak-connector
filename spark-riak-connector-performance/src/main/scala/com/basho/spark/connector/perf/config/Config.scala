package com.basho.spark.connector.perf.config

import com.typesafe.config.ConfigFactory
import java.io.File

/**
 * @author anekhaev
 */
trait Config { self: App =>
     
  lazy val config = if (args.length == 0) {
    ConfigFactory.parseResources("reference.conf").resolve()
  } else {
    ConfigFactory
      .parseFile(new File(args(0)))
      .withFallback(ConfigFactory.parseResources("reference.conf"))
      .resolve()
  }
  
}