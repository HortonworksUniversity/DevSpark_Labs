package solution

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount{
	def main(args: Array[String]) {
	
 	val conf = new SparkConf().setAppName("WordCount")
	conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
	conf.set("spark.sepeculation","true")

 	val sc = new SparkContext(conf)
	sc.setLogLevel("WARN")

 	val input = sc.textFile("/user/root/selfishgiant.txt")
 	val wc = input.flatMap(line => line.split(" ")).
 		map(line =>  (line,1)).reduceByKey((a,b) => a+b).
 		map{case (a,b) => (b,a)}.sortByKey(false)

	println("You submitted the Solution")
 	wc.take(10).foreach(println)
 	sc.stop()
 	}
} 

