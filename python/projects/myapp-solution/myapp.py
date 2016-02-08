import os
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	conf = SparkConf() \
		.setAppName("App Name") \
		.set("spark.speculation","true")
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")

	inputRdd = sc.textFile("/user/root/selfishgiant.txt").flatMap(lambda line: line.split(" ")).map(lambda line: (line,1))
	reducedRdd = inputRdd.reduceByKey(lambda a,b: a+b).map(lambda (a,b): (b,a)).sortByKey(ascending=False)	

	print(reducedRdd.take(10))
	sc.stop()

