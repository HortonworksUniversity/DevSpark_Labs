##Developer should start up a REPL using
#pyspark --master local[2]

#3a
from pyspark.streaming import StreamingContext

#3b
ssc = StreamingContext(sc, 5)

#3c
inputDS = ssc.socketTextStream("sandbox",9999)
#3d
wc = inputDS.flatMap(lambda line: line.split(" ")).map(lambda  word: (word,1)).reduceByKey(lambda a,b: a+b)

#3e
wc.pprint()

#3f
sc.setLogLevel("ERROR")

#3g
ssc.start()
