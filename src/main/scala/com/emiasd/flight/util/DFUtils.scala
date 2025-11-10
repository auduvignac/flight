// com/emiasd/flight/util/DFUtils.scala
package com.emiasd.flight.util

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object DFUtils {
  def addYearMonth(tsCol: String): Seq[Column] = Seq(
    year(col(tsCol)).as("year"),
    format_string("%02d", month(col(tsCol))).as("month")
  )

  def parseCrsTime(y: Column, m: Column, d: Column, hm: Column): Column = {
    val hhmm = lpad(hm.cast("string"), 4, "0")
    val hh   = substring(hhmm, 1, 2).cast("int")
    val mm   = substring(hhmm, 3, 2).cast("int")

    when(hm.isNull, lit(null).cast("timestamp"))
      .otherwise(
        make_timestamp(
          y.cast("int"),
          m.cast("int"),
          d.cast("int"),
          hh,
          mm,
          lit(0)
        )
      )

  }

  def safeCol(df: DataFrame, name: String, default: Column): Column =
    if (df.columns.contains(name)) col(name) else default

  def addMinutes(ts: Column, minutes: Column): Column =
    to_timestamp(from_unixtime(unix_timestamp(ts) + (minutes.cast("long") * 60L)))

  // FL_DATE (date) + HHmm (int/string) -> timestamp local (origine)
  def parseLocal(dateCol: Column, hhmmCol: Column): Column = {
    val hhmm = lpad(hhmmCol.cast("string"), 4, "0")
    to_timestamp(
      concat_ws(
        " ",
        date_format(dateCol, "yyyy-MM-dd"),
        concat(substring(hhmm, 1, 2), lit(":"), substring(hhmm, 3, 2))
      ),
      "yyyy-MM-dd HH:mm"
    )
  }
}
