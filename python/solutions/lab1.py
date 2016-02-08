sc
sc.appName
sc.master

baseRdd=sc.textFile("file:///root/spark/data/selfishgiant.txt")
splitRdd = baseRdd.flatMap(lambda line: line.split(" "))
mappedRdd = splitRdd.map(lambda line: (line,1))
reducedRdd = mappedRdd.reduceByKey(lambda a,b: a+b)

#Challenge:
reducedRdd.map(lambda (a,b): (b,a)).sortByKey(ascending=False).take(10)
