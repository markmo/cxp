package cxp

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.io.Source

/**
 * Created by markmo on 14/03/2015.
 */
object ExtractFeatures {

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("Web Analysis")
    val sc = new SparkContext(sparkConf)

    val logFile = "hdfs://localhost:9000/lab/cxp/output/events"

    // read features from dictionary
    val fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration())
    val dict = "hdfs://localhost:9000/lab/cxp/dictionary.csv"
    val in = fs.open(new Path(dict))
    val features = Source.fromInputStream(in).getLines().map(line => {
      line.split(",")(0)
    }).toList

    val events: RDD[CustomerEvent] = sc.textFile(logFile).map(EventParser.parseRecord)

    val output = "hdfs://localhost:9000/lab/cxp/output/features"
    try {
      fs.delete(new Path(output), true) // true for recursive
    } catch {
      case e:Throwable =>
        e.printStackTrace()
        println(e.getMessage)
    }
    events.map(event => ((event.customerId, event.attribute), (event.datetime, event.value, event.sessionId)))
      .reduceByKey((a, b) => if (a._1.compareTo(b._1) > 0) a else b)
      .map(event => (event._1._1, (event._1._2, event._2._1, event._2._2, event._2._3)))
      .groupByKey()
      .map(t => {
        val customerId = t._1
        val featureMap = t._2.map(t => (t._1, t._3.toString)).toMap
        val featureValues = features.map(feature => featureMap.getOrElse(feature, ""))
        s"$customerId,${featureValues.map(_.toString).mkString(",")}"
      })
      .saveAsTextFile(output)
  }
}
