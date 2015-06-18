The main idea of the OFAC demo is to show how Riak and Spark can be used to analyze semi-structured data using Python. According to the kdnuggets.com Python is way more popular than Scala within Data Science community: http://www.kdnuggets.com/polls/2014/languages-analytics-data-mining-data-science.html It has mature charting modules like matplotlib, and fast, feature-rich scientific tools like numpy and pandas.

As part of its enforcement efforts, Office of Foreign Assets Control (OFAC) publishes a list of individuals and companies owned or controlled by, or acting for or on behalf of, targeted countries. It also lists individuals, groups, and entities, such as terrorists and narcotics traffickers designated under programs that are not country-specific. Collectively, such individuals and companies are called "Specially Designated Nationals” (SDNs). Their assets are blocked and U.S. persons are generally prohibited from dealing with them. 

For this demo we need to download a public copy of the SDN list from OFAC: http://www.treasury.gov/ofac/downloads/sdn.csv
In order to connect SDNs to their specific locations we also need to get this address list: http://www.treasury.gov/ofac/downloads/add.csv

In this demo we are going to generate descriptive and summary statistics over OFAC dataset. As a part of our analysis we are going to calculate the following:

	•	How many unique SDNs do we have in total?
	•	Which distinct SDN Types does this dataset contain?
	•	How many banned individuals per country are in OFAC list? (supporting chart: bar plot)
	•	How many marine vessels are on the list by country and by vessel type? (supporting chart: heat map)
	•	What is the probability distribution of the vessel tonnage for vessels on the list? (histogram) 
	•	What are the most common titles of the individuals on the list? (word cloud chart)

We'll support our answers by drawing appropriate charts. 

## Prerequisites 

1. Install riak

2. Install python requirements:
```
sudo pip install -r requirements.txt
```

3. Install Apache Spark:
```
brew install apache-spark
```

4. Create an alias for your shell and run ipython notebook with pyspark session
```
echo "alias pyspark-ipython=\"IPYTHON_OPTS='notebook --pylab inline' pyspark --driver-memory 1000M\"" >> ~/.bashrc
source ~/.bashrc
pyspark-ipython
```