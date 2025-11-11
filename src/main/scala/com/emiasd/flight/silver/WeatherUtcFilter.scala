// com/emiasd/flight/silver/WeatherUtcFilter.scala
package com.emiasd.flight.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object WeatherUtcFilter {
  def toUtcAndFilter(
    weatherBronze: DataFrame,
    mappingWban: DataFrame,
    airports: DataFrame
  ): DataFrame = {

    // 1) WBAN -> (airport_id, timezone), puis UTC robuste (timezone null -> "UTC")
    val wWithAirport = weatherBronze
      .join(mappingWban, Seq("WBAN"), "left")
      .filter(col("airport_id").isNotNull)
      .withColumn(
        "obs_utc",
        to_utc_timestamp(
          col("obs_local_naive"),
          coalesce(col("timezone"), lit("UTC"))
        )
      )

    // 2) Restreindre aux aéroports présents dans les vols
    val airportSet = airports
      .select(col("origin_airport_id").as("aoi"))
      .union(airports.select(col("dest_airport_id").as("aoi")))
      .distinct()

    val filtered = wWithAirport
      .join(
        airportSet,
        wWithAirport("airport_id") === airportSet("aoi"),
        "inner"
      )
      .drop("aoi")

    // 3) Garantir year/month DÉRIVÉS DE obs_utc (pour le partitionnement Delta)
    val withYm = filtered
      .withColumn("year", year(col("obs_utc")))
      .withColumn("month", format_string("%02d", month(col("obs_utc"))))

    // 4) Sélection finale (tu peux ajouter/enlever des colonnes métier au besoin)
    withYm.select(
      col("airport_id"),
      col("timezone"),
      col("obs_local_naive"),
      col("obs_utc"),
      col("WBAN"),
      col("SkyCondition"),
      col("WeatherType"),
      col("Visibility"),
      col("DryBulbFarenheit"),
      col("DewPointFarenheit"),
      col("RelativeHumidity"),
      col("WindSpeed"),
      col("WindDirection"),
      col("Altimeter"),
      col("SeaLevelPressure"),
      col("StationPressure"),
      col("HourlyPrecip"),
      col("year"),
      col("month")
    )
  }

}
