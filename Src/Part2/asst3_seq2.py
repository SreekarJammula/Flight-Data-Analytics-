from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
import re


def output(value):
	return str(value.percentage)	

def split(value):
	return value[1].split(",")

conf = SparkConf()
sc = SparkContext(appName="Analysis Sequence")
sqlContext = SQLContext(sc)

seqfile = sc.sequenceFile("/bigd12/output_seq/")
seqfile2 = seqfile.map(split)
dfdata = sqlContext.createDataFrame(seqfile2, ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'CRSDepTime', 'ArrTime','CRSArrTime', 'UniqueCarrier', 'FlightNum', 'TailNum', 'ActualElapsedTime','CRSElapsedTime', 'AirTime', 'ArrDelay', 'DepDelay', 'Origin', 'Dest','Distance', 'TaxiIn', 'TaxiOut', 'Cancelled', 'CancellationCode', 'Diverted','CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay','LateAircraftDelay']
)
dfdata.createOrReplaceTempView("Seqtable")

query= sqlContext.sql("SELECT FlightNum,COUNT(*) AS Total,SUM(CASE WHEN DepDelay != 0 THEN 1 ELSE 0 END) AS DepDelay, (COUNT(*) * 1.0 /SUM(CASE WHEN DepDelay!= 0 THEN 1 ELSE 0 END)) * 100 as percentage FROM Seqtable GROUP BY Origin")
query2 = query.rdd.map(output)

query2.saveAsTextFile("/bigd12/output_seq2/")

