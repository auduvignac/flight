// com/emiasd/flight/bronze/WeatherBronze.scala
package com.emiasd.flight.bronze

import com.emiasd.flight.io.Readers
import com.emiasd.flight.io.Schemas.weatherKeep
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object WeatherBronze {
  def readAndEnrich(spark: SparkSession, inputs: Seq[String]): DataFrame = {

    val logger = Logger.getLogger(getClass.getName)

    logger.info(s"Lecture du fichier : $inputs")

    val raw =
      Readers.readTxt(spark, inputs, sep = ",", header = true, infer = true)

    // Debug: log sample of Date and Time columns to identify format
    logger.info("Sample Date/Time values from CSV:")
    raw.select(col("Date"), col("Time")).show(5, truncate = false)

    val df = raw
      .select(weatherKeep.map(c => col(c)): _*)
      .withColumn("WBAN", upper(trim(col("WBAN"))))
      // Date/Time -> 'obs_local_naive' (toujours local-naïf à ce stade)
      // Handle Date as integer (format: yyyyMMdd like 20120115) or string
      .withColumn("Date_str", col("Date").cast("string"))
      .withColumn(
        "Date_normalized",
        when(
          length(col("Date_str")) === 8, // yyyyMMdd format (e.g., 20120115)
          concat_ws(
            "-",
            substring(col("Date_str"), 1, 4), // year
            substring(col("Date_str"), 5, 2), // month
            substring(col("Date_str"), 7, 2)  // day
          )
        )
          .when(
            col("Date_str").rlike("^\\d{4}-\\d{2}-\\d{2}$"),
            col("Date_str")
          ) // Already yyyy-MM-dd
          .when(
            col("Date_str").rlike("^\\d{2}/\\d{2}/\\d{4}$"), // MM/dd/yyyy
            concat_ws(
              "-",
              substring(col("Date_str"), 7, 4), // year
              substring(col("Date_str"), 1, 2), // month
              substring(col("Date_str"), 4, 2)  // day
            )
          )
          .when(
            col("Date_str").rlike("^\\d{4}/\\d{2}/\\d{2}$"), // yyyy/MM/dd
            regexp_replace(col("Date_str"), "[/]", "-")
          )
          .otherwise(col("Date_str"))
      )
      .withColumn(
        "obs_local_naive",
        to_timestamp(
          concat_ws(
            " ",
            col("Date_normalized"),
            lpad(col("Time").cast("string"), 4, "0")
          ),
          "yyyy-MM-dd HHmm"
        )
      )
      .drop("Date_str", "Date_normalized")
      // Casts de base (standardisation lors de la phase Silver)
      .withColumn("Visibility", col("Visibility").cast("double"))
      .withColumn("DryBulbFarenheit", col("DryBulbFarenheit").cast("double"))
      .withColumn("DryBulbCelsius", col("DryBulbCelsius").cast("double"))
      .withColumn("DewPointFarenheit", col("DewPointFarenheit").cast("double"))
      .withColumn("DewPointCelsius", col("DewPointCelsius").cast("double"))
      .withColumn("WetBulbFarenheit", col("WetBulbFarenheit").cast("double"))
      .withColumn("WetBulbCelsius", col("WetBulbCelsius").cast("double"))
      .withColumn("RelativeHumidity", col("RelativeHumidity").cast("double"))
      .withColumn("WindSpeed", col("WindSpeed").cast("double"))
      .withColumn("WindDirection", col("WindDirection").cast("double"))
      .withColumn("Altimeter", col("Altimeter").cast("double"))
      .withColumn("SeaLevelPressure", col("SeaLevelPressure").cast("double"))
      .withColumn("StationPressure", col("StationPressure").cast("double"))
      .withColumn("HourlyPrecip", col("HourlyPrecip").cast("double"))
      .withColumn("year", year(col("obs_local_naive")))
      .withColumn("month", date_format(col("obs_local_naive"), "MM"))

    df
  }
}
