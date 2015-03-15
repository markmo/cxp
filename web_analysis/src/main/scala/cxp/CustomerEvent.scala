package cxp

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by markmo on 14/03/2015.
 */
case class CustomerEvent(customerId: String, attribute: String, datetime: Date, value: String, sessionId: String) extends Ordered[CustomerEvent] {
  import scala.math.Ordered.orderingToOrdered

  override
  def toString = {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    s"$customerId,$attribute,${df.format(datetime)},$value,$sessionId"
  }

  def compare(that: CustomerEvent): Int = (customerId, datetime) compare (that.customerId, that.datetime)
}