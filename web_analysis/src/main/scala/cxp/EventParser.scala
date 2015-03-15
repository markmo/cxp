package cxp

import java.text.SimpleDateFormat

/**
 * Created by markmo on 14/03/2015.
 */
object EventParser {

  def parseRecord(record: String) = {
    val columns = record.split(",")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    CustomerEvent(columns(0), columns(1), df.parse(columns(2)), columns(3), columns(4))
  }
}
