import csv
reader = csv.reader(open('/root/spark/data/carriers.csv'))
result = {}
for row in reader:
	key = row[0]
	result[key] = row[1]

