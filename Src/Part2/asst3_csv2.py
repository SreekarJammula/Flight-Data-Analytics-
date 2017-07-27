from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys
import re


def output(value):
	return str(value.percentage)

conf = SparkConf()
sc = SparkContext(conf=conf, appName="Analysis CSV")
sqlContext = SQLContext(sc)

csvFile = sqlContext.read.csv("/cosc6339_s17/flightdata-full/")
csvFile.registerTempTable("csvTable")

query= sqlContext.sql("SELECT FlightNum,COUNT(*) AS Total,SUM(CASE WHEN DepDelay != 0 THEN 1 ELSE 0 END) AS DepDelay, (COUNT(*) * 1.0 /SUM(CASE WHEN DepDelay!= 0 THEN 1 ELSE 0 END)) * 100 as percentage FROM csvTable GROUP BY Origin")

query2 = query.rdd.map(output)

query2.saveAsTextFile("/bigd12/outputcsv_2/")

