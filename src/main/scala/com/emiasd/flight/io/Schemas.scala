// com/emiasd/flight/io/Schemas.scala
package com.emiasd.flight.io


object Schemas {
  // Possibles schémas BTS et NCDC (restons permissifs, on lit en inferSchema côté Readers)
  val flightsSelected: Seq[String] = Seq(
    "FL_DATE",
    "OP_CARRIER_AIRLINE_ID",
    "OP_CARRIER_FL_NUM",
    "ORIGIN_AIRPORT_ID",
    "DEST_AIRPORT_ID",
    "CRS_DEP_TIME",
    "CRS_ELAPSED_TIME",
    "ARR_DELAY_NEW",
    "CANCELLED",
    "DIVERTED",
    "WEATHER_DELAY",
    "NAS_DELAY"
  )

  val weatherKeep: Seq[String] = Seq(
    "WBAN",
    "Date",
    "Time",
    // Ciel / phénomènes / visibilité
    "SkyCondition",
    "WeatherType",
    "Visibility",
    // Températures / humidité (on standardisera en °C en silver)
    "DryBulbFarenheit",
    "DryBulbCelsius",
    "DewPointFarenheit",
    "DewPointCelsius",
    "WetBulbFarenheit",
    "WetBulbCelsius",
    "RelativeHumidity",
    // Vent
    "WindSpeed",
    "WindDirection",
    // Pression
    "Altimeter",
    "SeaLevelPressure",
    "StationPressure",
    // Précipitations
    "HourlyPrecip"
  )
}
