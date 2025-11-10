// com/emiasd/flight/join/BuildJT.scala
package com.emiasd.flight.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BuildJT {
  // Colonnes météo alignées TIST
  private def selectWxCols(prefix: String) = Seq(
    col("obs_utc").as(s"${prefix}_ts"),
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
  )

  def buildJT(
    spark: SparkSession,
    flightsEnriched: DataFrame,
    weatherSlim: DataFrame,
    thMinutes: Int
  ): DataFrame = {
    // import spark.implicits._

    // wxOrigin
    val wxOrigin = weatherSlim.select(
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

    // wxDest
    val wxDest = weatherSlim.select(
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

    val f = flightsEnriched
      .filter(col("dep_ts_utc").isNotNull && col("arr_ts_utc").isNotNull)
      .withColumn("dep_minus_12h", col("dep_ts_utc") - expr("INTERVAL 12 HOURS"))
      .withColumn("arr_minus_12h", col("arr_ts_utc") - expr("INTERVAL 12 HOURS"))

    // Join météo origine (0..12h avant dep)
    val jO = f
      .join(
        wxOrigin,
        f("origin_airport_id") === wxOrigin("o_airport_id") &&
          wxOrigin("obs_utc").between(col("dep_minus_12h"), col("dep_ts_utc")),
        "left"
      )
      .select(f.columns.map(col) ++ selectWxCols("o"): _*)

    // --- origine ---
    val aggO = jO
      .groupBy(f.columns.map(col): _*)
      .agg(
        sort_array(
          collect_list(
            struct(
              col("o_ts"),
              col("o_sky"),
              col("o_wxType"),
              col("o_vis"),
              col("o_tempC"),
              col("o_dewC"),
              col("o_rh"),
              col("o_windKt"),
              col("o_windDir"),
              col("o_altim"),
              col("o_slp"),
              col("o_stnp"),
              col("o_precip")
            )
          ),
          asc = true
        ).as("Wo")
      )

    // Join météo destination (0..12h avant arr)
    val jD = aggO
      .join(
        wxDest,
        aggO("dest_airport_id") === wxDest("d_airport_id") &&
          wxDest("obs_utc").between(col("arr_minus_12h"), col("arr_ts_utc")),
        "left"
      )
      .select(aggO.columns.map(col) ++ selectWxCols("d"): _*)

    // --- destination ---
    val aggD = jD
      .groupBy(aggO.columns.map(col): _*)
      .agg(
        sort_array(
          collect_list(
            struct(
              col("d_ts"),
              col("d_sky"),
              col("d_wxType"),
              col("d_vis"),
              col("d_tempC"),
              col("d_dewC"),
              col("d_rh"),
              col("d_windKt"),
              col("d_windDir"),
              col("d_altim"),
              col("d_slp"),
              col("d_stnp"),
              col("d_precip")
            )
          ),
          asc = true
        ).as("Wd")
      )

    val withC =
      aggD.withColumn("C", when(col("ARR_DELAY_NEW") >= thMinutes, lit(1)).otherwise(lit(0)))

    // F struct minimal (tu pourras enrichir)
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
