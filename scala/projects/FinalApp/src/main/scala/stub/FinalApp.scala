package stub

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import au.com.bytecode.opencsv.CSVParser

object FinalApp{
        def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("FinalApp")

        val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
        
        //TODO, find average taxi time for each Carrier by Airline Manufacturer, save to HDFS
        val flight = sc.textFile(args(0)).map(line => line.split(","))
        val carrier = sc.textFile(args(1)).map(line => line.split(","))
        val planes = sc.textFile(args(2)).map(line => line.split(",")).filter(line => line.length == 9)
        
        val flightC = flight.keyBy(line => line(5))
        val carrierC = carrier.keyBy(line => line(0))
        
        val join1 = flightC.join
        
        val join1P = join1.map{case (a,b) => (b._1(7),(a,b._1,b._2))}
        val planesP = planes.keyBy(line => line(0))
        val finalRdd = join1P.join(planesP)
        val neededData = finalRdd.map{case (a,b) => ((b._1._3(1),b._2(2)),b._1._2(10).toInt)}
        val avgTaxByAir = neededData.groupByKey().mapValues(list => list.sum.toFloat/list.size)
        
        avgTaxByAir.saveAsTextFile(args(3))
        }
}
