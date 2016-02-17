#1a
type(sqlContext)

#2
sqlContext.sql("USE" flight")

#3
flights = sqlContext.table("flights")
planes = sqlContext.table("planes")

#4
flights.sort("distance", ascending=False).take(1)

#5
longflights = flights.filter(flights.distance==4962).select("tailnum").distinct()

#6
longflightplanes = longflights.join(planes, 'tailnum' , 'inner')

#7
longflightplanes.select("model").groupBy("model").count().show()
