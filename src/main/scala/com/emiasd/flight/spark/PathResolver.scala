// com/emiasd/flight/spark/PathResolver.scala
package com.emiasd.flight.spark

import com.emiasd.flight.config.{AppConfig, Environment}

final case class IOPaths(
  flightsInputs: Seq[String],
  weatherInputs: Seq[String],
  mapping: String,
  bronzeFlights: String,
  bronzeWeather: String,
  silverFlights: String,
  silverWeatherFiltered: String,
  goldJT: String
)

object PathResolver {
  def resolve(cfg: AppConfig): IOPaths = {
    val (fDir, wDir, map, bronze, silver, gold) = cfg.env match {
      case Environment.Local =>
        (
          cfg.inFlightsDir,
          cfg.inWeatherDir,
          cfg.inMapping,
          cfg.deltaBronzeBase,
          cfg.deltaSilverBase,
          cfg.deltaGoldBase
        )
      case Environment.Hadoop =>
        (
          cfg.hInFlightsDir,
          cfg.hInWeatherDir,
          cfg.hInMapping,
          cfg.hDeltaBronzeBase,
          cfg.hDeltaSilverBase,
          cfg.hDeltaGoldBase
        )
    }
    val flightsInputs = cfg.monthsF.map(m => s"$fDir/$m.csv")
    val weatherInputs = cfg.monthsW.map(m => s"$wDir/${m}hourly.txt")
    IOPaths(
      flightsInputs,
      weatherInputs,
      map,
      s"$bronze/flights",
      s"$bronze/weather",
      s"$silver/flights",
      s"$silver/weather_filtered",
      s"$gold/JT_th${cfg.thMinutes}"
    )
  }
}
