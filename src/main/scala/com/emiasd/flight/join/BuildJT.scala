package com.emiasd.flight.join

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object BuildJT {

  private def hourlyArrayClosest(
    fKeyTs: DataFrame,
    wx: DataFrame,
    airportKey: String,
    prefix: String,
    toleranceMin: Int
  ): DataFrame = {

    val targetCol = s"${prefix}_target"

    val expanded = fKeyTs
      .withColumn(
        "ts_grid",
        sequence(
          col("ts") - expr("INTERVAL 12 HOURS"),
          col("ts"),
          expr("INTERVAL 1 HOURS")
        )
      )
      .withColumn(targetCol, explode(col("ts_grid")))
      .drop("ts_grid")

    val joined = expanded.join(
      wx,
      col(airportKey) === col(s"${prefix}_airport_id") &&
        col("obs_utc").between(
          col(targetCol) - expr(s"INTERVAL $toleranceMin MINUTES"),
          col(targetCol) + expr(s"INTERVAL $toleranceMin MINUTES")
        ),
      "left"
    )

    val w = Window
      .partitionBy(col("flight_key"), col(targetCol))
      .orderBy(abs(col("obs_utc").cast("long") - col(targetCol).cast("long")))

    val pointCol = s"${prefix}_point"

    val best = joined
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1)
      .drop("rn")
      .select(
        col("flight_key"),
        struct(
          col(targetCol).as(s"${prefix}_ts"),
          col("SkyCondition").as(s"${prefix}_sky"),
          col("WeatherType").as(s"${prefix}_wxType"),
          col("Visibility").cast("double").as(s"${prefix}_vis"),
          col("TempC").cast("double").as(s"${prefix}_tempC"),
          col("DewPointC").cast("double").as(s"${prefix}_dewC"),
          col("RelativeHumidity").cast("double").as(s"${prefix}_rh"),
          col("WindSpeedKt").cast("double").as(s"${prefix}_windKt"),
          col("WindDirection").cast("double").as(s"${prefix}_windDir"),
          col("Altimeter").cast("double").as(s"${prefix}_altim"),
          col("SeaLevelPressure").cast("double").as(s"${prefix}_slp"),
          col("StationPressure").cast("double").as(s"${prefix}_stnp"),
          col("HourlyPrecip").cast("double").as(s"${prefix}_precip")
        ).as(pointCol)
      )

    val outCol = if (prefix == "o") "Wo" else "Wd"

    best
      .groupBy("flight_key")
      .agg(sort_array(collect_list(col(pointCol)), asc = false).as(outCol))
  }

  // ======================================================================
  // buildJT — VERSION AVEC ÉTAPE 1 (repartition sur flight_key)
  // ======================================================================

  def buildJT(
    flightsEnriched: DataFrame,
    weatherSlim: DataFrame,
    thMinutes: Int,
    toleranceMin: Int = 45
  ): DataFrame = {

    // -----------------------
    // Préparation météo
    // -----------------------
    val wx = weatherSlim
      .repartition(col("airport_id"))
      .sortWithinPartitions(col("airport_id"), col("obs_utc"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val wxO = wx.select(
      col("airport_id").as("o_airport_id"),
      col("obs_utc"),
      col("SkyCondition"),
      col("WeatherType"),
      col("Visibility"),
      col("TempC"),
      col("DewPointC"),
      col("RelativeHumidity"),
      col("WindSpeedKt"),
      col("WindDirection"),
      col("Altimeter"),
      col("SeaLevelPressure"),
      col("StationPressure"),
      col("HourlyPrecip")
    )

    val wxD = wx.select(
      col("airport_id").as("d_airport_id"),
      col("obs_utc"),
      col("SkyCondition"),
      col("WeatherType"),
      col("Visibility"),
      col("TempC"),
      col("DewPointC"),
      col("RelativeHumidity"),
      col("WindSpeedKt"),
      col("WindDirection"),
      col("Altimeter"),
      col("SeaLevelPressure"),
      col("StationPressure"),
      col("HourlyPrecip")
    )

    // -----------------------
    // Extraction données vol
    // -----------------------
    val f0 = flightsEnriched
      .filter(col("dep_ts_utc").isNotNull && col("arr_ts_utc").isNotNull)
      .select(
        col("flight_key"),
        col("origin_airport_id"),
        col("dest_airport_id"),
        col("dep_ts_utc"),
        col("arr_ts_utc"),
        col("ARR_DELAY_NEW"),
        col("OP_CARRIER_AIRLINE_ID"),
        col("FL_NUM"),
        col("FL_DATE"),
        col("CRS_DEP_TIME"),
        col("NAS_DELAY"),
        col("WEATHER_DELAY"),
        col("NAS_WEATHER_DELAY"),
        col("year"),
        col("month")
      )

    // 1) Origin features
    val fO = f0.select(
      col("flight_key"),
      col("origin_airport_id"),
      col("dep_ts_utc").as("ts")
    )

    val originFeatures = hourlyArrayClosest(
      fO,
      wxO,
      "origin_airport_id",
      "o",
      toleranceMin
    )

    // 2) Dest features
    val fD = f0.select(
      col("flight_key"),
      col("dest_airport_id"),
      col("arr_ts_utc").as("ts")
    )

    val destFeatures = hourlyArrayClosest(
      fD,
      wxD,
      "dest_airport_id",
      "d",
      toleranceMin
    )

    wx.unpersist()

    // ======================================================
    // ÉTAPE 1 : Repartition alignée sur flight_key
    // ======================================================
    val f0p             = f0.repartition(col("flight_key"))
    val originFeaturesP = originFeatures.repartition(col("flight_key"))
    val destFeaturesP   = destFeatures.repartition(col("flight_key"))

    // -----------------------
    // Jointure finale JT
    // -----------------------
    val withWx = f0p
      .join(originFeaturesP, "flight_key")
      .join(destFeaturesP, "flight_key")

    // label C
    val withC = withWx.withColumn(
      "C",
      when(col("ARR_DELAY_NEW") >= lit(thMinutes), lit(1)).otherwise(lit(0))
    )

    // structure finale JT
    withC.select(
      struct(
        col("OP_CARRIER_AIRLINE_ID").as("carrier"),
        col("FL_NUM").as("flnum"),
        col("FL_DATE").as("date"),
        col("origin_airport_id"),
        col("dest_airport_id"),
        col("CRS_DEP_TIME").as("crs_dep_scheduled_hhmm"),
        col("dep_ts_utc"),
        col("arr_ts_utc"),
        col("ARR_DELAY_NEW").as("arr_delay_new"),
        col("WEATHER_DELAY").as("weather_delay"),
        col("NAS_DELAY").as("nas_delay"),
        col("NAS_WEATHER_DELAY").as("nas_weather_delay"),
        col("flight_key")
      ).as("F"),
      col("Wo"),
      col("Wd"),
      col("C"),
      col("year"),
      col("month")
    )
  }
}
