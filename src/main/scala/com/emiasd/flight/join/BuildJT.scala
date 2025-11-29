// com/emiasd/flight/join/BuildJT.scala
package com.emiasd.flight.join

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp

object BuildJT {

  private def hourlyArrayClosest(
    fBase: DataFrame,
    wxPrefixed: DataFrame,
    airportColInF: String,
    tsColInF: String,
    prefix: String,
    toleranceMin: Int = 45
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

    // 2) Join météo ± tolérance
    val joined = fWithGrid.join(
      wxPrefixed,
      col(airportColInF) === col(s"${prefix}_airport_id") &&
        col("obs_utc").between(
          col(s"${prefix}_target") - expr(s"INTERVAL ${toleranceMin} MINUTES"),
          col(s"${prefix}_target") + expr(s"INTERVAL ${toleranceMin} MINUTES")
        ),
      "left"
    )

    // 3) Sélectionner l'observation la plus proche
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

    // 4) Recomposer le tableau trié
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
    val base = buildJTBase(flightsEnriched, weatherSlim, toleranceMin)
    attachLabel(base, thMinutes)
  }

  /**
   * Pipeline commun vols/météo sans application du seuil.
   */
  def buildJTBase(
    flightsEnriched: DataFrame,
    weatherSlim: DataFrame,
    toleranceMin: Int = 45
  ): DataFrame = {

    // Préparation des bornes temporelles
    val fWithBounds = flightsEnriched
      .filter(col("dep_ts_utc").isNotNull && col("arr_ts_utc").isNotNull)
      .withColumn(
        "dep_minus_12h",
        col("dep_ts_utc") - expr("INTERVAL 12 HOURS")
      )
      .withColumn("arr_plus_12h", col("arr_ts_utc") + expr("INTERVAL 12 HOURS"))

    val boundsOpt = fWithBounds
      .agg(
        min(col("dep_minus_12h")).as("min_ts"),
        max(col("arr_plus_12h")).as("max_ts")
      )
      .collect()
      .headOption
      .flatMap { row =>
        val minTs = Option(row.getAs[Timestamp]("min_ts"))
        val maxTs = Option(row.getAs[Timestamp]("max_ts"))
        for { min <- minTs; max <- maxTs } yield (min, max)
      }

    val f = fWithBounds
      .drop("dep_minus_12h", "arr_plus_12h")
      .select(
        col("origin_airport_id"),
        col("dest_airport_id"),
        col("dep_ts_utc"),
        col("arr_ts_utc"),
        col("ARR_DELAY_NEW"),
        col("WEATHER_DELAY"),
        col("NAS_DELAY"),
        col("NAS_WEATHER_DELAY"),
        col("OP_CARRIER_AIRLINE_ID"),
        col("FL_NUM"),
        col("FL_DATE"),
        col("CRS_DEP_TIME"),
        col("flight_key"),
        col("year"),
        col("month")
      )

    val weatherPruned = boundsOpt.map { case (minTs, maxTs) =>
      weatherSlim.filter(col("obs_utc").between(lit(minTs), lit(maxTs)))
    }
      .getOrElse(weatherSlim)

    // ========================================
    //   OPTIMISATION CRITIQUE : partitionnement fin
    // ========================================
    val wxBase = weatherPruned
      .repartitionByRange(200, col("airport_id"), col("obs_utc"))
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

    // ========= VOL ORIGIN : repartitionByRange pour éliminer le skew =========
    val fForO = f.repartitionByRange(200, col("origin_airport_id"))

    val withWo = hourlyArrayClosest(
      fForO,
      wxOrigin.withColumnRenamed("o_airport_id", "o_airport_id"),
      airportColInF = "origin_airport_id",
      tsColInF = "dep_ts_utc",
      prefix = "o",
      toleranceMin = toleranceMin
    )

    // ========= VOL DESTINATION =========
    val fForD = withWo.repartitionByRange(200, col("dest_airport_id"))

    val withWd = hourlyArrayClosest(
      fForD,
      wxDest.withColumnRenamed("d_airport_id", "d_airport_id"),
      airportColInF = "dest_airport_id",
      tsColInF = "arr_ts_utc",
      prefix = "d",
      toleranceMin = toleranceMin
    )

    wxBase.unpersist()

    withWd
  }

  /**
   * Applique le seuil et compose la sortie finale.
   */
  def attachLabel(jtBase: DataFrame, thMinutes: Int): DataFrame = {
    val withC = jtBase.withColumn(
      "C",
      when(col("ARR_DELAY_NEW") >= lit(thMinutes), lit(1)).otherwise(lit(0))
    )

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
