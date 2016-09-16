# Getting the Spark-Riak Connector

**Note: Please be sure to check the lastest release version by going to https://github.com/basho/spark-riak-connector/releases/latest)**

If you are using Scala or Java, the best way to get the Spark-Riak Connector library to use with your project is to have SBT or Maven download the library at build time. This can be done by including the coordinates of the Spark-Riak Connector library in the your `build.sbt` or `pom.xml` file. You can also manually download the Spark-Riak Connector library before build time and include it in a local repository.

If you are using Python, the best way to get the Spark-Riak Connector is to download the library and include its location in the Spark classpath when submitting your project to Spark.

An alternative way to get the Spark-Riak Connector library for Scala, Java, and Python is to include the Spark-Riak Connector library in the packages option when submitting your app to Spark. The Spark-Riak Connector will automatically be downloaded from spark-packages.org.

## Scala

To add the Spark-Riak Connector to your Scala project, add the following to your `build.sbt` file:

```
libraryDependencies +=  "com.basho.riak" %% "spark-riak-connector" % "$version"
          exclude("org.apache.spark", "spark-core")
          exclude("org.apache.spark", "spark-sql")
```

## Java

To add the Spark-Riak Connector to your Java project, add the following to your `pom.xml` file:

```
<dependencies>
    <dependency>
        <groupId>com.basho.riak</groupId>
        <artifactId>spark-riak-connector</artifactId>
        <version>$version</version>
    </dependency>
</dependencies>
```

## Python

**Be sure to check the latest release version by going to https://github.com/basho/spark-riak-connector/releases/latest**

You can download the Spark-Riak Connector library by going to https://github.com/basho/spark-riak-connector/releases/latest and clicking on the latest release jar file.

Here's another way to download and install it in your home directory:

```
curl -sS -O http://repo1.maven.org/maven2/com/basho/riak/spark-riak-connector_{{scala-version}}/{{connector-version}}/spark-riak-connector_{{scala-version}}-{{connector-version}}-uber.jar 
```
You will need to replace `{{connector-version}}` with the latest version or the Spark-Riak Connector. You will also need to replace `{{scala-version}}` with `2.10` or `2.11` depending on your version of scala. Once you've downloaded the connector, you can add it to the driver classpath when submitting your Python app to Spark, like this:

```
/path/to/spark-submit \
    --master "local[*]" \
    --driver-class-path /path/to/spark-riak-connector_{{scala-version}}-{{connector-version}}-uber.jar \
    /path/to/your-python-script.py
```

#### Alternative (Scala, Python, Java)

As an alternative, you can add the Spark-Riak Connector library in the packages option when submitting the app to Spark. The Spark-Riak Connector can be found on spark-packages.org. You can either download it and supply spark with its local address or have spark automatically download it when starting up the spark shell as show by the following.

```
$SPARK_HOME/bin/spark-submit \
--packages com.basho.riak:spark-riak-connector_{{scala-version}}:{{connector-version}}
```

**Scala**
```
$SPARK_HOME/bin/spark-shell \
--packages com.basho.riak:spark-riak-connector_{{scala-version}}:{{connector-version}}
```

**Python**
```
$SPARK_HOME/bin/pyspark \
--packages com.basho.riak:spark-riak-connector_{{scala-version}}:{{connector-version}}
```
