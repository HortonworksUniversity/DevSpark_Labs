##Developer should start REPL using:
#pyspark --master local[2]

#3a
from pyspark.streaming import StreamingContext

#3b
ssc = StreamingContext(sc, 2)

#3c
inputDS = ssc.socketTextStream("sandbox",9999)

#3d
ssc.checkpoint("hdfs:///user/root/checkpointDir")

#3e
windowDS = inputDS.window(10,2).flatMap(lambda line:line.split(" ")) \
.map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)

#3f
windowDS.pprint()

#3g
sc.setLogLevel("ERROR")

#3h
ssc.start()
