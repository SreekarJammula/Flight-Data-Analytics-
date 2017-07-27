from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys
import re

def output(value):
	return str(value.percentage)

conf = SparkConf()
sc = SparkContext(conf=conf, appName="Analysis Parquet")
sqlContext = SQLContext(sc)

parquetFile = sqlContext.read.parquet("/bigd12/ouputpar/")
parquetFile.registerTempTable("parTable")

query= sqlContext.sql("SELECT FlightNum,COUNT(*) AS Total,SUM(CASE WHEN DepDelay != 0 THEN 1 ELSE 0 END) AS DepDelay, (COUNT(*) * 1.0 /SUM(CASE WHEN DepDelay!= 0 THEN 1 ELSE 0 END)) * 100 as percentage FROM parTable GROUP BY Origin")

query2 = query.rdd.map(output)

query2.saveAsTextFile("/bigd12/outputpar_2/")
