// com/emiasd/flight/analysis/TargetsInspection.scala
package com.emiasd.flight.analysis

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TargetsInspection {

  val logger = Logger.getLogger(getClass.getName)

  def inspectSlice(
    spark: SparkSession,
    df: DataFrame,
    dsValue: String = "D2",
    thValue: Int = 60,
    n: Int = 20
  ): Unit = {

    logger.info(s"=== Targets slice ds=$dsValue, th=$thValue ===")

    val slice = df.filter(col("ds") === dsValue && col("th") === thValue)

    slice.groupBy("is_pos").count().orderBy("is_pos").show()

    slice
      .select(
        col("F.carrier").as("carrier"),
        col("F.flnum").as("flnum"),
        col("F.date").as("date"),
        col("C"),
        col("is_pos")
      )
      .show(n, truncate = false)
  }
}
