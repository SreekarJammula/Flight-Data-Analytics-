from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys
import re

conf = SparkConf()
sc = SparkContext(conf=conf, appName="CSVtoSeq")
sqlContext = SQLContext(sc)


def airlineTuple(line):
    values = line.split(",")

    return ('', values[0]+" "+values[1]+" "+values[2]+" "+values[3]+" "+values[4]+" "+values[5]+" "+values[6]+" "+ values[7]+" "+ values[8]+" "+ values[9]+" "+ values[10]+" "+ values[11]+" "+ values[12]+" "+ values[13]+" "+ values[14]+" "+ values[15]+" "+ values[16]+" "+ values[17]+" "+ values[18]+" "+ values[19]+" "+  values[20]+" "+ values[21]+" "+ values[22]+" "+ values[23]+" "+ values[24]+" "+ values[25]+" "+ values[26]+" "+ values[27]+" "+ values[28]+"\n")


lines = sc.textFile("/cosc6339_s17/flightdata-full/").map(airlineTuple).saveAsSequenceFile("/bigd12/output_seq/")









