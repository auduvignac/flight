// com/emiasd/flight/join/WxJoinHelpers.scala
package com.emiasd.flight.join

import com.emiasd.flight.util.DFUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object WxJoinHelpers {

  /** Structure météo robuste (null si absent), prête pour Wo/Wd */
  def buildWxStruct(df: DataFrame, prefix: String): Column = {
    def nullExpr = expr(
      "CAST(NULL AS DOUBLE)"
    ) // helper pour éviter la répétition

    struct(
      safeCol(df, "obs_utc", expr("CAST(NULL AS TIMESTAMP)"))
        .as(s"${prefix}_ts"),
      safeCol(df, "SkyCondition", expr("CAST(NULL AS STRING)"))
        .as(s"${prefix}_sky"),
      safeCol(df, "WeatherType", expr("CAST(NULL AS STRING)"))
        .as(s"${prefix}_wxType"),
      safeCol(df, "Visibility", nullExpr).as(s"${prefix}_vis"),
      safeCol(df, "TempC", nullExpr).as(s"${prefix}_tempC"),
      safeCol(df, "DewPointC", nullExpr).as(s"${prefix}_dewC"),
      safeCol(df, "RelativeHumidity", nullExpr).as(s"${prefix}_rh"),
      safeCol(df, "WindSpeedKt", nullExpr).as(s"${prefix}_windKt"),
      safeCol(df, "WindDirection", nullExpr).as(s"${prefix}_windDir"),
      safeCol(df, "Altimeter", nullExpr).as(s"${prefix}_altim"),
      safeCol(df, "SeaLevelPressure", nullExpr).as(s"${prefix}_slp"),
      safeCol(df, "StationPressure", nullExpr).as(s"${prefix}_stnp"),
      safeCol(df, "HourlyPrecip", nullExpr).as(s"${prefix}_precip")
    )
  }

  /** Teste la présence d'une colonne dans un DataFrame. */
  def has(df: DataFrame, colName: String): Boolean =
    df.columns.contains(colName)
}
