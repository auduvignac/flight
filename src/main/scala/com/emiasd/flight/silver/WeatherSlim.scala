// com/emiasd/flight/silver/WeatherSlim.scala
package com.emiasd.flight.silver

import com.emiasd.flight.io.Readers
import com.emiasd.flight.util.DFUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object WeatherSlim {

  def enrichWithUTC(
    spark: SparkSession,
    weatherBronze: DataFrame,
    wbanTzPath: String
  ): DataFrame = {

    val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

    // Validation: count rows before join
    val countBefore = weatherBronze.count()
    logger.info(s"[WeatherSlim] Weather Bronze rows before join: $countBefore")

    val tz = Readers
      .readCsv(spark, Seq(wbanTzPath))
      .select(
        col("WBAN").cast("string").as("WBAN"),
        col("AirportID").cast("int").as("airport_id"),
        col("TimeZone").cast("int").as("tz_hour")
      )
      .dropDuplicates("WBAN")

    logger.info(
      s"[WeatherSlim] Timezone mapping contains ${tz.count()} unique WBANs"
    )

    // INNER join: Only keep weather observations with valid airport_id mapping
    // This filters out weather stations not associated with airports
    // Expected behavior: ~7-8% data loss (stations without airport mapping)
    val joined = weatherBronze.join(tz, Seq("WBAN"), "inner")

    // Validation: count rows after join and warn if excessive loss
    val countAfter = joined.count()
    val lossRate   = 100.0 * (countBefore - countAfter) / countBefore

    logger.info(s"[WeatherSlim] Weather rows after INNER join: $countAfter")
    logger.info(
      s"[WeatherSlim] Data loss: ${countBefore - countAfter} rows (${lossRate}%.2f%%)"
    )

    if (lossRate > 15.0) {
      logger.warn(
        s"⚠️ INNER join dropped ${lossRate}%.2f%% of weather data! " +
          s"Expected <15%. Check timezone mapping file: $wbanTzPath"
      )
    } else {
      logger.info(s"✓ Data loss is acceptable (${lossRate}%.2f%% < 15%)")
    }

    val df = joined
      .withColumn("tz_offset_min", coalesce(col("tz_hour") * lit(60), lit(0)))
      .withColumn(
        "obs_utc",
        addMinutes(col("obs_local_naive"), -col("tz_offset_min"))
      )
      // ==== Standardisation des unités ====
      // TempC (°C) prioritaire; sinon converti depuis °F
      .withColumn(
        "TempC",
        coalesce(
          col("DryBulbCelsius"),
          (col("DryBulbFarenheit") - lit(32.0)) * lit(5.0 / 9.0)
        )
      )
      .withColumn(
        "DewPointC",
        coalesce(
          col("DewPointCelsius"),
          (col("DewPointFarenheit") - lit(32.0)) * lit(5.0 / 9.0)
        )
      )
      // Vent : on garde la vitesse telle quelle (les QCLCD sont souvent en mph ou kt selon source).
      // Si tu sais l’unité exacte de ton CSV, convertis ici vers m/s ou kt.
      .withColumnRenamed("WindSpeed", "WindSpeedRaw")
      .withColumn(
        "WindSpeedKt",
        col("WindSpeedRaw")
      ) // TODO: convertir si nécessaire
      // Pression : on garde les 3 mesures disponibles (Altimeter inHg, SLP/StationPressure hPa)
      //
      .withColumn("year", year(col("obs_utc")))
      .withColumn("month", date_format(col("obs_utc"), "MM"))
      .select(
        col("airport_id"),
        col("WBAN"),
        col("obs_utc"),
        col("SkyCondition"),
        col("WeatherType"),
        col("Visibility").cast("double"),
        col("TempC").cast("double"),
        col("DewPointC").cast("double"),
        col("RelativeHumidity").cast("double"),
        col("WindSpeedKt").cast("double"),
        col("WindDirection").cast("double"),
        col("Altimeter").cast("double"),
        col("SeaLevelPressure").cast("double"),
        col("StationPressure").cast("double"),
        col("HourlyPrecip").cast("double"),
        col("year"),
        col("month")
      )
      .filter(col("airport_id").isNotNull)

    df
  }
}
