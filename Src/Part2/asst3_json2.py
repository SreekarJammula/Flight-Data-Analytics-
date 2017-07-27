from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json
import sys
import re

def output(value):
	return str(value.percentage)

conf = SparkConf()
sc = SparkContext(conf=conf, appName="Analysis JSON")
sqlContext = SQLContext(sc)

jsonFile = sqlContext.read.json("/bigd12/outputjson2/")
jsonFile.registerTempTable("jsonTable")

query= sqlContext.sql("SELECT FlightNum,COUNT(*) AS Total,SUM(CASE WHEN DepDelay != 0 THEN 1 ELSE 0 END) AS DepDelay, (COUNT(*) * 1.0 /SUM(CASE WHEN DepDelay!= 0 THEN 1 ELSE 0 END)) * 100 as percentage FROM jsonTable GROUP BY Origin")

query2 = query.rdd.map(output)

query2.saveAsTextFile("/bigd12/outputjson_2/")
