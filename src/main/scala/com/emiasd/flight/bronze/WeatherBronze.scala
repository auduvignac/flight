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

    val raw = Readers.readTxt(spark, inputs, sep = ",", header = true, infer = true)

    val df = raw
      .select(weatherKeep.map(c => col(c)): _*)
      .withColumn("WBAN", upper(trim(col("WBAN"))))
      // Date/Time -> 'obs_local_naive' (toujours local-naïf à ce stade)
      .withColumn("Date", regexp_replace(col("Date"), "[/]", "-"))
      .withColumn(
        "obs_local_naive",
        to_timestamp(concat_ws(" ", col("Date"), lpad(col("Time"), 4, "0")), "yyyy-MM-dd HHmm")
      )
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
