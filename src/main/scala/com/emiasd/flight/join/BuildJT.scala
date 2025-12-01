// com/emiasd/flight/join/BuildJT.scala
package com.emiasd.flight.join

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

object BuildJT {

  private def hourlyArrayClosest(
    fBase: DataFrame,
    wxPrefixed: DataFrame,
    airportColInF: String,
    tsColInF: String,
    prefix: String,
    toleranceMin: Int = 20
  ): DataFrame = {

    // 1) Grille horaire: 13 timestamps cibles [ts-12h .. ts] pas 1h
    val targetsColName = s"${prefix}_targets"
    val targetColName  = s"${prefix}_target"

    val fWithGrid = fBase
      .withColumn(
        targetsColName,
        sequence(
          col(tsColInF) - expr("INTERVAL 12 HOURS"),
          col(tsColInF),
          expr("INTERVAL 1 HOURS")
        )
      )
      .withColumn(targetColName, explode(col(targetsColName)))
      .drop(col(targetsColName))

    // 2) Candidats météo dans la tolérance ±toleranceMin
    val joined = fWithGrid.join(
      wxPrefixed,
      col(airportColInF) === col(s"${prefix}_airport_id") &&
        col("obs_utc").between(
          col(s"${prefix}_target") - expr(s"INTERVAL ${toleranceMin} MINUTES"),
          col(s"${prefix}_target") + expr(s"INTERVAL ${toleranceMin} MINUTES")
        ),
      "left"
    )

    // 3) Garder l'observation la plus proche pour chaque (vol, heure cible)
    val partCols: Seq[Column] =
      fBase.columns.map(col) :+ col(s"${prefix}_target")
    val w = Window
      .partitionBy(partCols: _*)
      .orderBy(
        abs(col("obs_utc").cast("long") - col(s"${prefix}_target").cast("long"))
      )

    val pointCol = s"${prefix}_point"
    val best = joined
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1)
      .select(
        (fBase.columns.map(col) :+
          struct(
            col(s"${prefix}_target").as(s"${prefix}_ts"),
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
          ).as(pointCol)): _*
      )

    // 4) Recomposer l'array trié (ts, ts-1h, ..., ts-12h)
    val outCol = if (prefix == "o") "Wo" else "Wd"
    best
      .groupBy(fBase.columns.map(col): _*)
      .agg(sort_array(collect_list(col(pointCol)), asc = false).as(outCol))
  }

  def buildJT(
    flightsEnriched: DataFrame,
    weatherSlim: DataFrame,
    thMinutes: Int,
    toleranceMin: Int = 45
  ): DataFrame = {

    // ========= OPTIMS MÉTÉO =========
    val wxBase = weatherSlim
      .repartition(col("airport_id"))
      .sortWithinPartitions(col("airport_id"), col("obs_utc"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val wxOrigin = wxBase.select(
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

    val wxDest = wxBase.select(
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

    // ========= PRÉPARATION VOLS =========
    val f0 = flightsEnriched
      .filter(col("dep_ts_utc").isNotNull && col("arr_ts_utc").isNotNull)
      .withColumn(
        "dep_minus_12h",
        col("dep_ts_utc") - expr("INTERVAL 12 HOURS")
      )
      .withColumn(
        "arr_minus_12h",
        col("arr_ts_utc") - expr("INTERVAL 12 HOURS")
      )

    // Répartition pour limiter le shuffle
    val fForO = f0.repartition(col("origin_airport_id"))

    // ========= ORIGINE : Wo (closest hourly) =========
    val withWo = hourlyArrayClosest(
      fForO,
      wxOrigin.withColumnRenamed("o_airport_id", "o_airport_id"),
      airportColInF = "origin_airport_id",
      tsColInF = "dep_ts_utc",
      prefix = "o",
      toleranceMin = toleranceMin
    )

    val fForD = withWo.repartition(col("dest_airport_id"))

    // ========= DESTINATION : Wd (closest hourly) =========
    val withWd = hourlyArrayClosest(
      fForD,
      wxDest.withColumnRenamed("d_airport_id", "d_airport_id"),
      airportColInF = "dest_airport_id",
      tsColInF = "arr_ts_utc",
      prefix = "d",
      toleranceMin = toleranceMin
    )

    wxBase.unpersist()

    // ========= ÉTIQUETTE C =========
    val withC = withWd.withColumn(
      "C",
      when(col("ARR_DELAY_NEW") >= lit(thMinutes), lit(1)).otherwise(lit(0))
    )

    // ========= STRUCT F =========
    val withF = withC.select(
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

    withF
  }
}
