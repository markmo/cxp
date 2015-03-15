package cxp

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by markmo on 14/03/2015.
 */
object ParseAccessLog {

  private val p = Pattern.compile("JSESSIONID=([^&]+)")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Web Analysis")
    val sc = new SparkContext(sparkConf)

    val logFile = "hdfs://localhost:9000/lab/cxp/www1/access.log"

    val parser = new AccessLogParser

    val accessLogs: RDD[Option[AccessLogRecord]] = sc.textFile(logFile).map(parser.parseRecord)

    val formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss")

    val events: RDD[CustomerEvent] = accessLogs.collect({
      case Some(log) =>
        val request = log.request
        val parts = request.split(" ")
        val value = parts(1)
        val sessionId = extractSessionId(value)
        CustomerEvent(log.clientIpAddress, "web_request", formatter.parse(log.dateTime.substring(1, log.dateTime.length - 1)), value, sessionId.getOrElse(""))
    })

    val output = "hdfs://localhost:9000/lab/cxp/output/events"
    val fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration())
    try {
      fs.delete(new Path(output), true) // true for recursive
    } catch {
      case e:Throwable =>
        e.printStackTrace()
        println(e.getMessage)
    }
    val sessionLengths = sessionLengthSeconds(events)
    val now = new Date
    events.union(pageViewsPerSession(events).map(event =>
        CustomerEvent(event._1._2, "page_views_per_session", event._2._1, event._2._2.toString, event._1._1)))
      .union(sessionLengths.map(event =>
        CustomerEvent(event._1._2, "session_length_seconds", event._2._1, event._2._2.toString, event._1._1)))
      .union(averageSessionLengthSeconds(sessionLengths).map(t =>
        CustomerEvent(t._1, "average_session_length_seconds", now, t._2.toString, "NA")))
      .sortBy(event => (event.customerId, event.datetime))
      .saveAsTextFile(output)
  }

  // Feature generators

  def pageViewsPerSession(events: RDD[CustomerEvent]): RDD[((String, String), (Date, Int))] =
    events.map(event => ((event.sessionId, event.customerId), (event.datetime, 1)))
      .reduceByKey((a, b) => (max(a._1, b._1), a._2 + b._2))

  def sessionLengthSeconds(events: RDD[CustomerEvent]): RDD[((String, String), (Date, Long))] =
    events.map(event => ((event.sessionId, event.customerId), (event.datetime, event.datetime)))
      .reduceByKey((a, b) => (min(a._1, b._1), max(a._2, b._2)))
      .map(event => (event._1, (event._2._2, (event._2._2.getTime - event._2._1.getTime)/1000)))

  /**
   *
   * @param rdd ((sessionId, customerId), (sessionEndDate, sessionLengthSeconds))
   * @return
   */
  def averageSessionLengthSeconds(rdd: RDD[((String, String), (Date, Long))]): RDD[(String, Float)] =
    rdd
      .map(t => (t._1._2, (t._1._1, t._2._1, t._2._2)))
      .groupByKey()
      .map(t => (t._1, t._2.map(_._3).reduceLeft(_ + _)/t._2.size))

  // Utility functions

  def extractSessionId(request: String): Option[String] = {
    val matcher = p.matcher(request)
    if (matcher.find) {
      Some(matcher.group(1))
    } else {
      None
    }
  }

  def min(a: Date, b: Date) = if (a.compareTo(b) > 0) b else a

  def max(a: Date, b: Date) = if (a.compareTo(b) > 0) a else b
}
