# Examples and Interactive Scala Shell (REPL)
Riak Spark connector comes with several sample programs and demos:

- [Simple scala example](.#Simple-Scala-example)
- [Simple java example](.#Simple-Java-example)
- [OFAC demo](.#OFAC-demo)

And also it provides helper to run [bootstrapped Spark Shell](.#Interactive-Scala-Shell) which is ready to go.
 
## Building and Running examples/demos
1. Those examples assume that you have a BDP-enhanced version of Riak KV cluster installed and running on localhost:8087. If not, follow this guide to install Riak: [Installing and Upgrading Riak](http://docs.basho.com/riak/latest/ops/building/installing/). Note that currently only Riak KV bundled with BDP 1.0 is supported with Spark Connector.

2. If you don't have Maven yet, go to [Maven download page](https://maven.apache.org/download.cgi) and follow installation instructions for your OS.

3. Install dependencies (we skip integration tests to speed things up):
```
mvn install -DskipTests
```

4. Extract REPL/Examples
```
unzip spark-riak-connector-examples/target/spark-riak-connector-examples-1.0.0-REPL.zip
```

5. Run the Example/Demo

To run an example use `./bin/run-example <class> [params]`. For example:
    ./bin/run-example SimpleScalaRiakExample

will run the SimpleScalaRiakExample example locally.

## Interactive Scala Shell
The easiest way to start using Spark is through the Scala shell:
```
./bin/rspark-shell
```

It will run the original Spark Shell bootstrapped with all necessary [imports](./src/main/repl/conf/rspark-shell-defaults.scala)
and with the proper classpath. 

## Simple Scala example
A quite simple example that demonstrates how to use Riak Spark connector to query all data from the corresponding  bucket. 
This example creates test data in the Riak, pull it back to the Spark side by utilizing RiakRDD and, as a result, 
it calculates the number of values loaded from the Riak bucket.

Sources [SimpleScalaRiakExample.scala](./src/main/scala/com/basho/riak/spark/examples/SimpleScalaRiakExample.scala)

## Simple Java example
Similar to [Simple scala example](.#Simple Scala example) but  demonstrating Java usage.

Sources [SimpleScalaRiakExample.scala](./src/main/java/com/basho/riak/spark/examples/SimpleJavaRiakExample.scala)

## OFAC demo
The main idea of the demo is to show how Riak and Spark can be used to analyze semi-structured data using Scala.

As part of its enforcement efforts, Office of Foreign Assets Control (OFAC) publishes a list of individuals and companies owned or controlled by, or acting for or on behalf of, targeted countries. It also lists individuals, groups, and entities, such as terrorists and narcotics traffickers designated under programs that are not country-specific. Collectively, such individuals and companies are called "Specially Designated Nationals” (SDNs). Their assets are blocked and U.S. persons are generally prohibited from dealing with them.

More information about OFAC list can be found at [www.treasury.gov](http://www.treasury.gov/resource-center/sanctions/SDN-List/Pages/default.aspx)

### Initial Data
For the demo we need to download a public copy of the [SDN list from OFAC](http://www.treasury.gov/ofac/downloads/sdn.csv).
In order to connect SDNs to their specific locations we also need to get an [address list](http://www.treasury.gov/ofac/downloads/add.csv).
As well as we need  to download [Stop words list](https://en.wikipedia.org/wiki/Stop_words) for the proper word frequency analysis.

All the data required for this demo will be downloaded automatically from the public internet copies.

### Calculated Metrics
In this demo we are going to generate descriptive and summary statistics over OFAC dataset. As a part of our analysis we are going to calculate the following:

* How many unique SDNs do we have in total?
* Which distinct SDN Types does this dataset contain?
* How many banned individuals per country are in OFAC list? (supporting chart: bar plot)
* How many marine vessels are on the list by country and by vessel type? (supporting chart: heat map)
* What is the probability distribution of the vessel tonnage for vessels on the list? (histogram) 
* What are the most common titles of the individuals on the list? (word cloud chart)

We'll support our answers by drawing appropriate charts. 

Sources [OFACDemo.scala](./src/main/scala/com/basho/riak/spark/examples/demos/ofac/OFACDemo.scala)
