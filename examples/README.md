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

### Running demo on Scala

1. This demo assumes that you have a BDP-enhanced version of Riak KV cluster installed and running on localhost:8087. If not, follow this guide to install Riak: [Installing and Upgrading Riak](http://docs.basho.com/riak/latest/ops/building/installing/). Note that currently only Riak KV bundled with BDP 1.0 is supported with Spark Connector.

2. If you don't have Maven yet, go to [Maven download page](https://maven.apache.org/download.cgi) and follow installation instructions for your OS.

3. Install dependencies (we skip integration tests to speed things up):
```
mvn install -DskipTests
```

4. Extract REPL
```
unzip spark-riak-connector-examples/target/spark-riak-connector-examples-1.0.0-REPL.zip
```

5. Run the Demo
```
cd spark-riak-connector-examples-1.0.0
./rspark-submit
```
