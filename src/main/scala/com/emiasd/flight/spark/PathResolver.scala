package com.emiasd.flight.spark

import com.emiasd.flight.config.{AppConfig, Environment}

/** Chemins résolus dynamiquement selon l'environnement et la base Delta. */
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
    // 1) Base Delta effective
    val effectiveDeltaBase: String = cfg.deltaBase.getOrElse {
      // fallback : garde le schéma “/app/delta/{bronze|silver|gold}”
      cfg.env match {
        case Environment.Hadoop => cfg.hDeltaGoldBase.dropRight("/gold".length)
        case _                  => cfg.deltaGoldBase.dropRight("/gold".length)
      }
    }

    // 2) Entrées
    val (fDir, wDir, map) = cfg.env match {
      case Environment.Local =>
        (cfg.inFlightsDir, cfg.inWeatherDir, cfg.inMapping)
      case Environment.Hadoop =>
        (cfg.hInFlightsDir, cfg.hInWeatherDir, cfg.hInMapping)
    }
    val flightsInputs = cfg.monthsF.map(m => s"$fDir/$m.csv")
    val weatherInputs = cfg.monthsW.map(m => s"$wDir/${m}hourly.txt")

    // 3) Sous-arborescences Delta (dérivées de la base effective)
    val bronzeBase = s"$effectiveDeltaBase/bronze"
    val silverBase = s"$effectiveDeltaBase/silver"
    val goldBase   = s"$effectiveDeltaBase/gold"

    IOPaths(
      flightsInputs = flightsInputs,
      weatherInputs = weatherInputs,
      mapping = map,
      bronzeFlights = s"$bronzeBase/flights",
      bronzeWeather = s"$bronzeBase/weather",
      silverFlights = s"$silverBase/flights",
      silverWeatherFiltered = s"$silverBase/weather_filtered",
      goldJT = s"$goldBase/JT_th${cfg.thMinutes}"
    )
  }
}
