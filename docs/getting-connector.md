# Getting the Spark-Riak Connector

Before using any of the commands in this section, please be sure to check the lastest release version by going to https://github.com/basho/spark-riak-connector/releases/latest). Furthermore, you will need to replace `{{connector-version}}` with the latest version of the Spark-Riak Connector, and you will also need to replace `{{scala-version}}` with `2.10` or `2.11` depending on your version of scala. Example commands do not include neccesary options such as `--master`. 

Here are three ways to use the Spark-Riak Connector in your application:

## `--packages` option

You can add the Spark-Riak Connector library in the packages option when using `spark-submit`, `spark-shell`, or `pyspark`. The Spark-Riak Connector can be found on spark-packages.org. Spark will automatically download the connector and make it available with your app when starting up Spark as show by the following:

**Scala, Python, and Java**
```
$SPARK_HOME/bin/spark-submit \
--packages com.basho.riak:spark-riak-connector_{{scala-version}}:{{connector-version}}

$SPARK_HOME/bin/spark-shell \
--packages com.basho.riak:spark-riak-connector_{{scala-version}}:{{connector-version}}

$SPARK_HOME/bin/pyspark \
--packages com.basho.riak:spark-riak-connector_{{scala-version}}:{{connector-version}}
```

## `--driver-class-path` option

To use the `--driver-class-path` option you will first to get the Spark-Riak connector jar on your local machine. This can be accomplished in the following ways:

- Download the uber jar from Maven Central: `http://repo1.maven.org/maven2/com/basho/riak/spark-riak-connector_{{scala-version}}/{{connector-version}}/spark-riak-connector_{{scala-version}}-{{connector-version}}-uber.jar `

- Build the connector, please see [building and testing the connector](./building-and-testing-connector.md)

Once you have a local connector jar, you can use it with the `--driver-class-path` option as follows:

**Scala, Python, and Java**
```
$SPARK_HOME/bin/spark-submit \
    --driver-class-path /path/to/local/spark-riak-connector_{{scala-version}}-{{connector-version}}-uber.jar

$SPARK_HOME/bin/spark-shell \
    --driver-class-path /path/to/local/spark-riak-connector_{{scala-version}}-{{connector-version}}-uber.jar

$SPARK_HOME/bin/pyspark \
    --driver-class-path /path/to/local/spark-riak-connector_{{scala-version}}-{{connector-version}}-uber.jar
```

## `build.sbt` or `pom.xml`

If you are using Scala or Java, you can use the Spark-Riak connector with your project by having SBT or Maven download the library at build time. This can be done by including the coordinates of the Spark-Riak Connector library in the your `build.sbt` or `pom.xml` file. You can also manually download the Spark-Riak Connector library before build time and include it in a local repository.

###Scala

To add the Spark-Riak Connector to your Scala project, add the following to your `build.sbt` file:

```
libraryDependencies +=  "com.basho.riak" %% "spark-riak-connector" % "$version"
          exclude("org.apache.spark", "spark-core")
          exclude("org.apache.spark", "spark-sql")
```

### Java

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
