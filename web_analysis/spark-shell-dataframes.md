val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

case class Customer(customerId: String, webRequest: String, pageViewsPerSession: Int, sessionLengthSeconds: Int, averageSessionLengthSeconds: Double)

val data = sc.textFile("hdfs://localhost:9000/lab/cxp/output/features").map(_.split(",")).map(r => Customer(r(0), r(1), r(2).trim.toInt, r(3).trim.toInt, r(4).trim.toDouble))