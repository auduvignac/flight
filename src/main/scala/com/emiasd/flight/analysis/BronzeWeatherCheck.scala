// com/emiasd/flight/analysis/BronzeWeatherCheck.scala
package com.emiasd.flight.analysis

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BronzeWeatherCheck {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def checkBronzeWeather(
    spark: SparkSession,
    bronzeWeatherPath: String
  ): Unit = {
    logger.info("=== BRONZE WEATHER DATA CHECK ===")

    val weatherDF = spark.read.format("delta").load(bronzeWeatherPath)

    // Show schema
    logger.info("\n--- Weather Bronze Schema ---")
    weatherDF.printSchema()

    // Count total rows
    val totalRows = weatherDF.count()
    logger.info(s"\nTotal weather rows: $totalRows")

    // Check NULL rates for key columns
    logger.info("\n--- NULL Rates for Key Weather Fields (Bronze) ---")

    val keyFields = Seq(
      "Visibility",
      "DryBulbFarenheit",
      "DryBulbCelsius",
      "DewPointFarenheit",
      "DewPointCelsius",
      "RelativeHumidity",
      "WindSpeed",
      "WindDirection",
      "Altimeter",
      "SeaLevelPressure",
      "StationPressure",
      "HourlyPrecip",
      "SkyCondition",
      "WeatherType"
    )

    keyFields.foreach { field =>
      if (weatherDF.columns.contains(field)) {
        val nullCount = weatherDF.filter(col(field).isNull).count()
        val pct       = 100.0 * nullCount / totalRows
        logger.info(f"  $field%-25s : $nullCount%8d nulls ($pct%6.2f%%)")
      } else {
        logger.warn(f"  $field%-25s : COLUMN NOT FOUND!")
      }
    }

    // Sample actual data
    logger.info("\n--- Sample Bronze Weather Data (first 10 rows) ---")
    weatherDF
      .select(
        col("WBAN"),
        col("obs_local_naive"),
        col("Visibility"),
        col("DryBulbCelsius"),
        col("WindSpeed"),
        col("HourlyPrecip")
      )
      .show(10, truncate = false)

    // Check distinct WBANs
    val distinctWBAN = weatherDF.select(col("WBAN")).distinct().count()
    logger.info(s"\nDistinct WBANs in Bronze: $distinctWBAN")
  }
}
