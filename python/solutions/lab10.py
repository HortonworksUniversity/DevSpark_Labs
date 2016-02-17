from pyspark import Row

flightRdd=sc.textFile("/user/root/flights.csv").map(lambda line: line.split(","))

flightORdd=flightRdd.map(lambda f: Row(Month=int(f[0]), \
DayOfMonth=int(f[1]), \
DayofWeek=int(f[2]), \
DepTime=int(f[3]), \
ArrTime=int(f[4]), \
UniqueCarrier=f[5], \
FlightNum=f[6], \
TailNum= f[7], \
ElapsedTime=int(f[8]), \
AirTime=int(f[9]), \
ArrDelay=int(f[10]), \
DepDelay=int(f[11]), \
Origin=f[12], \
Dest=f[13], \
Distance=int(f[14]), \
TaxiIn=int(f[15]), \
TaxiOut=int(f[16]), \
Cancelled=f[17], \
CancellationCode=f[18], \
Diverted=f[19]))

flightDF=sqlContext.createDataFrame(flightORdd)
flightDF.printSchema()

flightDF.write.format("parquet").save("/user/root/flights.parquet")

#4
dfflight=sqlContext.read.format("parquet").load("/user/root/flights.parquet")
dfflight.printSchema()

#5a
dfflight.select(dfflight.Origin, dfflight.DepDelay) \
.groupBy('Origin').avg() \
.withColumnRenamed("AVG(DepDelay)",  "DelayAvg") \
.sort('DelayAvg', ascending=False).show()

#5bi
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def delay_check(x):
  if x > 0:
    return 1
  else:
    return 0

depUDF = udf(delay_check, IntegerType())

#5bii
delayDF = dfflight.select(dfflight.UniqueCarrier, depUDF(dfflight.DepDelay).alias("IsDelayed"), dfflight.DepDelay)

#5biii
delayGroupDF = delayDF.groupBy(delayDF.UniqueCarrier) \
.agg({"IsDelayed": "sum", "DepDelay": "count"})

#5biv
from pyspark.sql.types import FloatType
calc_percent = udf(lambda s,c: (float(s)/c), FloatType())

#5bv
delayGroupDF.select(delayGroupDF.UniqueCarrier,calc_percent("SUM(IsDelayed)","COUNT(DepDelay)").alias("Percentage")) \
.sort("Percentage", ascending=False).show()

#5c
dfflight.select("UniqueCarrier", "Distance") \
.groupBy("UniqueCarrier").avg() \
.sort("AVG(Distance)", ascending=False).show(5)

#6a
dfflight.select("Origin", "TaxiIn") \
.groupBy("Origin").avg() \
.sort("AVG(TaxiIn)", ascending=False).show(5)

#6b
dfflight.select("Origin", "TaxiOut") \
.groupBy("Origin").avg() \
.sort("AVG(TaxiOut)", ascending=True).show(5)




