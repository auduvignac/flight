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

    // === Sélection de la base Delta effective ===
    // Si --deltaBase est fourni en CLI, il a priorité.
    val deltaBase = cfg.deltaBase.getOrElse {
      cfg.env match {
        case Environment.Hadoop => cfg.hDeltaGoldBase
        case _                  => cfg.deltaGoldBase
      }
    }

    // === Sélection des répertoires d'entrée ===
    val (fDir, wDir, map) = cfg.env match {
      case Environment.Local =>
        (cfg.inFlightsDir, cfg.inWeatherDir, cfg.inMapping)
      case Environment.Hadoop =>
        (cfg.hInFlightsDir, cfg.hInWeatherDir, cfg.hInMapping)
    }

    // === Résolution des chemins d'entrée ===
    val flightsInputs = cfg.monthsF.map(m => s"$fDir/$m.csv")
    val weatherInputs = cfg.monthsW.map(m => s"$wDir/${m}hourly.txt")

    // === Résolution des chemins Delta ===
    val bronzeBase = deltaBase.replace("gold", "bronze")
    val silverBase = deltaBase.replace("gold", "silver")

    IOPaths(
      flightsInputs = flightsInputs,
      weatherInputs = weatherInputs,
      mapping = map,
      bronzeFlights = s"$bronzeBase/flights",
      bronzeWeather = s"$bronzeBase/weather",
      silverFlights = s"$silverBase/flights",
      silverWeatherFiltered = s"$silverBase/weather_filtered",
      goldJT = s"$deltaBase/JT_th${cfg.thMinutes}"
    )
  }
}
