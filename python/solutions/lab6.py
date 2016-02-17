data = sc.parallelize([1,2,3,4,5])

for x in range(100):
  data = data.map(lambda i: i+1)
  
print(data.toDebugString())

data = sc.parallelize([1,2,3,4,5])

for x in range(600):
  data = data.map(lambda i: i+1)
  
print(data.toDebugString())
data.take(1)

sc.setCheckpointDir("checkpointDir")
data = sc.parallelize([1,2,3,4,5])
for x in range(100):
  if x%7 == 0:
    data.checkpoint()	
 	data=data.map(lambda i: i+1)

print(data.toDebugString())
data.take(1)
