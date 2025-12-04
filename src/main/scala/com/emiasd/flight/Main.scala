package com.emiasd.flight

// =======================
// Imports
// =======================
import com.emiasd.flight.analysis.{BronzeAnalysis, SilverAnalysis, TargetsInspection, WeatherFeatureDiagnostics}
import com.emiasd.flight.bronze.{FlightsBronze, WeatherBronze}
import com.emiasd.flight.config.{AppConfig, Environment}
import com.emiasd.flight.io.{Readers, Writers}
import com.emiasd.flight.join.{BuildJT, FlightsEnriched}
import com.emiasd.flight.ml.FeatureBuilder.FeatureConfig
import com.emiasd.flight.ml.{ExperimentConfig, FeatureBuilder, ModelingPipeline}
import com.emiasd.flight.silver.{CleaningPlans, WeatherSlim}
import com.emiasd.flight.spark.{IOPaths, PathResolver}
import com.emiasd.flight.targets.TargetBatch
import org.apache.log4j.Logger
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import scopt.OParser

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Point d'entrée principal pour exécuter le pipeline ETL (Bronze → Silver →
 * Gold)
 */
object Main {

  // =======================
  // Logger
  // =======================
  val logger: Logger = Logger.getLogger(getClass.getName)
  private val sparkListenerRegistered =
    new java.util.concurrent.atomic.AtomicBoolean(false)

  final case class BronzeData(flights: DataFrame, weather: DataFrame)
  final case class SilverData(flights: DataFrame, weatherSlim: DataFrame)
  final case class GoldData(targets: DataFrame)

  // =======================
  // Étape 1 : BRONZE
  // =======================
  def runBronze(
    spark: SparkSession,
    paths: IOPaths,
    debug: Boolean,
    persist: Boolean
  ): BronzeData = {
    logger.info("=== Étape BRONZE ===")

    // Lecture et enrichissement des vols
    logger.info("Lecture et enrichissement des vols (FlightsBronze)")
    val flightsBronze =
      FlightsBronze.readAndEnrich(spark, paths.flightsInputs, paths.mapping)

    // Vérification de l'unicité des colonnes
    logger.info("Vérification de l'unicité des colonnes")
    val dupCols = flightsBronze.columns.groupBy(_.toLowerCase).collect {
      case (n, arr) if arr.length > 1 => n
    }
    require(dupCols.isEmpty, s"Duplicate columns: ${dupCols.mkString(", ")}")

    // Lecture et enrichissement météo
    logger.info("Lecture et enrichissement météo (WeatherBronze)")
    val weatherBronze =
      WeatherBronze.readAndEnrich(spark, paths.weatherInputs, debug)

    // Préparation des repartitions alignées sur (year, month) pour éviter les sur-coalesce
    val flightsBronzeForWrite =
      flightsBronze.repartition(col("year"), col("month"))
    val weatherBronzeForWrite =
      weatherBronze.repartition(col("year"), col("month"))

    // Écriture en Delta Lake
    if (persist) {
      logger.info("Écriture des tables BRONZE en Delta Lake")
      Writers.writeDelta(
        flightsBronzeForWrite,
        paths.bronzeFlights,
        Seq("year", "month"),
        overwriteSchema = true
      )
      Writers.writeDelta(
        weatherBronzeForWrite,
        paths.bronzeWeather,
        Seq("year", "month"),
        overwriteSchema = true
      )
    } else {
      logger.info(
        "Persist désactivé — Materialisation de flightsBronze et weatherBronze."
      )
      flightsBronze.cache(); flightsBronze.count()
      weatherBronze.cache(); weatherBronze.count()
    }

    if (debug) {
      val bronzeQaDir = s"${paths.analysisDir}/bronze"
      Writers.mkdirSmart(spark, bronzeQaDir)(logger)
      logger.info("Analyse qualité Bronze : vols et météo")
      BronzeAnalysis.analyzeFlights(flightsBronze, bronzeQaDir)
      BronzeAnalysis.analyzeWeather(weatherBronze, bronzeQaDir)
    } else {
      logger.info("Mode debug désactivé — analyses Bronze ignorées.")
    }

    logger.info("Étape Bronze terminée avec succès.")
    BronzeData(flightsBronze, weatherBronze)
  }

  // =======================
  // Étape 2 : SILVER
  // =======================
  def runSilver(
    spark: SparkSession,
    paths: IOPaths,
    debug: Boolean,
    persist: Boolean,
    bronzeData: Option[BronzeData] = None
  ): SilverData = {
    logger.info("=== Étape SILVER ===")

    // Vérification/lecture des tables Bronze
    val resolvedBronze: BronzeData =
      bronzeData.getOrElse {
        if (persist) {
          val bronzeFlightsExists = Readers.exists(spark, paths.bronzeFlights)
          val bronzeWeatherExists = Readers.exists(spark, paths.bronzeWeather)

          if (!bronzeFlightsExists || !bronzeWeatherExists) {
            logger.warn(
              "Tables Bronze manquantes — lancement automatique de runBronze()."
            )
            runBronze(spark, paths, debug, persist)
          } else {
            logger.info(
              "Lecture des tables BRONZE (flights & weather) depuis Delta"
            )
            BronzeData(
              Readers.readDelta(spark, paths.bronzeFlights),
              Readers.readDelta(spark, paths.bronzeWeather)
            )
          }
        } else {
          logger.info(
            "Persist désactivé — recalcul des tables Bronze en mémoire."
          )
          runBronze(spark, paths, debug, persist = false)
        }
      }

    val flightsBronze = resolvedBronze.flights
    val weatherBronze = resolvedBronze.weather

    // Nettoyage et enrichissement des vols
    logger.info("Nettoyage et enrichissement des vols (CleaningPlans)")
    val flightsSilver = CleaningPlans.cleanFlights(flightsBronze)

    // Écriture de la table Silver Flights
    if (persist) {
      logger.info("Écriture des données Silver Flights")
      Writers.writeDelta(
        flightsSilver.coalesce(2),
        paths.silverFlights,
        Seq("year", "month"),
        overwriteSchema = true
      )
    } else {
      logger.info("Persist désactivé — saut de l'écriture Silver Flights.")
    }

    if (debug) {
      val silverQaDir = s"${paths.analysisDir}/silver"
      Writers.mkdirSmart(spark, silverQaDir)(logger)
      logger.info("Analyse qualité Silver : vols nettoyés")
      val flightsSilverForQa =
        if (persist) Readers.readDelta(spark, paths.silverFlights)
        else flightsSilver
      SilverAnalysis.analyzeFlights(flightsSilverForQa, silverQaDir)
    } else {
      logger.info("Mode debug désactivé — analyses Silver ignorées.")
    }

    // Enrichissement météo (UTC simplifié)
    logger.info("Enrichissement météo (WeatherSlim.enrichWithUTC)")
    val weatherSlim =
      WeatherSlim.enrichWithUTC(spark, weatherBronze, paths.mapping, debug)

    // Écriture de la météo Silver
    if (persist) {
      logger.info("Écriture des données Silver Weather Filtered")
      Writers.writeDelta(
        weatherSlim.coalesce(2),
        paths.silverWeatherFiltered,
        Seq("year", "month"),
        overwriteSchema = true
      )
    } else {
      logger.info(
        "Persist désactivé — Materialisation de flightsSilver et weatherSlim."
      )
      flightsSilver.cache(); flightsSilver.count()
      weatherSlim.cache(); weatherSlim.count()
    }

    logger.info("Étape Silver terminée avec succès.")
    SilverData(flightsSilver, weatherSlim)
  }

  // =======================
  // Étape 3 : GOLD
  // =======================
  def runGold(
    spark: SparkSession,
    paths: IOPaths,
    cfg: AppConfig,
    debug: Boolean,
    persist: Boolean,
    silverData: Option[SilverData] = None
  ): GoldData = {

    logger.info("=== Étape GOLD ===")

    // Résolution des tables Silver
    val resolvedSilver: SilverData =
      silverData.getOrElse {
        val flightsExists = Readers.exists(spark, paths.silverFlights)
        val weatherExists = Readers.exists(spark, paths.silverWeatherFiltered)

        if (persist) {
          if (!flightsExists || !weatherExists) {
            logger.warn(
              "Tables Silver absentes — recalcul complet (persist=true)."
            )
            runSilver(spark, paths, debug, persist = true)
          } else {
            logger.info("Lecture SILVER depuis Delta (persist=true).")
            SilverData(
              Readers.readDelta(spark, paths.silverFlights),
              Readers.readDelta(spark, paths.silverWeatherFiltered)
            )
          }
        } else {
          if (!flightsExists || !weatherExists) {
            logger.warn(
              "Tables Silver absentes — recalcul en mémoire (persist=false)."
            )
            runSilver(spark, paths, debug, persist = false)
          } else {
            logger.info(
              "Persist désactivé — lecture SILVER depuis Delta (persist=false)."
            )
            SilverData(
              Readers.readDelta(spark, paths.silverFlights),
              Readers.readDelta(spark, paths.silverWeatherFiltered)
            )
          }
        }
      }

    val flightsPrepared = resolvedSilver.flights
    val weatherSlimDF   = resolvedSilver.weatherSlim

    // Enrichissement Flights
    logger.info("Enrichissement des vols (FlightsEnriched)")
    val flightsEnriched = FlightsEnriched.build(flightsPrepared)

    // Liste des thresholds
    val thresholds: Seq[Int] = cfg.th match {
      case Some(th) => Seq(th)
      case None     => Seq(15, 30, 45, 60, 90)
    }
    val thRef = thresholds.head

    val goldBase =
      paths.goldJT.substring(0, paths.goldJT.lastIndexOf('/'))

    // Construction JT Base + Label
    logger.info("Construction de la base JT (BuildJT.buildJTBase)")
    val jtBase = BuildJT.buildJTBase(flightsEnriched, weatherSlimDF)

    logger.info(
      s"Ajout du label C dans JT pour le seuil de référence th=$thRef"
    )
    val jtLabeled = BuildJT.attachLabel(jtBase, thRef)

    // Matérialisation JT (en mémoire dans tous les cas)
    val jtRef: DataFrame = {
      if (!persist) {
        logger.info(
          "Persist désactivé — matérialisation de la JT en cache (MEMORY_AND_DISK)."
        )
      } else {
        logger.info(
          "Persist activé — matérialisation de la JT en cache (MEMORY_AND_DISK) pour réutilisation écriture + targets."
        )
      }
      val df = jtLabeled.persist(StorageLevel.MEMORY_AND_DISK)
      df.count() // coupe le DAG
      df
    }

    // Écriture (optionnelle) de JT → correction : pas de relecture Delta
    val jtForTargets: DataFrame =
      if (persist) {
        logger.info(
          s"Écriture de la JT de référence en Delta : ${paths.goldJT}"
        )

        val jtForWrite =
          if (cfg.env == Environment.Hadoop) {
            logger.info(
              "Env = Hadoop : repartition JT par (year, month) avant écriture"
            )
            jtRef.repartition(col("year"), col("month"))
          } else {
            logger.info(
              "Env = Local : pas de repartition explicite sur JT (AQE décide)."
            )
            jtRef
          }

        Writers.writeDelta(
          jtForWrite,
          paths.goldJT,
          Seq("year", "month"),
          overwriteSchema = true
        )

        logger.info(
          "Persist=true — réutilisation directe de jtRef en mémoire (pas de relecture Delta)."
        )
        jtRef
      } else {
        logger.info(
          "Persist désactivé — aucune écriture de JT en Delta, utilisation directe en mémoire."
        )
        jtRef
      }

    // Génération des keys & targets
    val tau = 0.95

    logger.info(
      s"Construction des keys pour thresholds=${thresholds.mkString(",")} (tau=$tau)"
    )
    val keysAll =
      TargetBatch.buildKeysForThresholds(
        jtForTargets,
        thresholds,
        tau,
        sampleSeed = 1234L
      )

    logger.info(
      "Matérialisation complète des targets (TargetBatch.materializeAll)"
    )
    val fullAll =
      TargetBatch.materializeAll(
        jtForTargets,
        keysAll,
        includeLightCols = true
      )

    logger.info("Sélection des colonnes finales pour targetsDf")
    val targetsDfRaw = fullAll.select(
      col("F"),
      col("Wo"),
      col("Wd"),
      col("C"),
      col("flight_key"),
      col("year"),
      col("month"),
      col("ds"),
      col("th"),
      col("is_pos")
    )

    // Cache targets si persist=false
    val targetsDf: DataFrame =
      if (!persist) {
        logger.info(
          "Cache de targetsDf en mémoire (MEMORY_AND_DISK, persist=false)"
        )
        val df = targetsDfRaw.persist(StorageLevel.MEMORY_AND_DISK)
        df.count()
        df
      } else targetsDfRaw

    // Écriture targets
    if (persist) {
      val out = s"$goldBase/targets"

      val targetsPartitioned =
        if (
          targetsDf.columns.contains("year") &&
          targetsDf.columns.contains("month")
        )
          targetsDf.repartition(col("ds"), col("th"), col("year"), col("month"))
        else
          targetsDf.repartition(col("ds"), col("th"))

      logger.info(s"Écriture des targets en Delta : $out")
      Writers.writeDelta(
        targetsPartitioned,
        out,
        Seq("ds", "th", "year", "month"),
        overwriteSchema = true
      )
    } else {
      logger.info("Persist désactivé — aucune écriture de targets en Delta.")
    }

    // Debug facultatif
    if (debug) {
      logger.info(
        "Inspection d'un slice de targets (TargetsInspection.inspectSlice)"
      )
      TargetsInspection.inspectSlice(
        spark,
        targetsDf,
        dsValue = "D2",
        thValue = thresholds.head,
        n = 20
      )
    } else {
      logger.info("Mode debug désactivé — inspection des targets ignorée.")
    }

    logger.info("Étape Gold avec succès.")
    GoldData(targetsDf)
  }

  // =======================
  // Étape 4 : DIAGNOSTICS
  // =======================
  def runDiagnostics(
    spark: SparkSession,
    paths: IOPaths,
    cfg: AppConfig,
    debug: Boolean,
    persist: Boolean
  ): Unit = {
    logger.info("=== Étape DIAGNOSTICS ===")

    if (!persist) {
      logger.warn(
        "Persist désactivé — les diagnostics nécessitent des tables Gold écrites. Étape ignorée."
      )
    } else {

      // Vérification de la présence de la table Gold
      val goldJTExists = Readers.exists(spark, paths.goldJT)

      if (!goldJTExists) {
        logger.warn(
          "Aucune table Gold trouvée — lancement automatique de runGold()"
        )
        runGold(spark, paths, cfg, debug, persist)
      } else {
        logger.info(
          "La table Gold est présente — passage direct aux diagnostics."
        )
      }

      // Extraction des paramètres pour les diagnostics
      val ds          = cfg.ds.getOrElse("D2")
      val th          = cfg.th.getOrElse(60)
      val originHours = cfg.originHours.getOrElse(7)
      val destHours   = cfg.destHours.getOrElse(7)

      logger.info(
        s"Lancement des diagnostics météo : ds=$ds, th=$th, originHours=$originHours, destHours=$destHours"
      )

      if (debug) {
        WeatherFeatureDiagnostics.runDiagnostics(
          spark,
          paths.goldJT,
          paths.silverWeatherFiltered,
          paths.silverFlights,
          paths.bronzeWeather,
          ds,
          th,
          originHours,
          destHours
        )
      } else {
        logger.info("Mode debug désactivé — diagnostics météo ignorés.")
      }

      logger.info("=== Étape DIAGNOSTICS terminée ===")
    }
  }

  // =======================
  // Étape 5 : SPARK ML
  // =======================
  def runModeling(
    spark: SparkSession,
    paths: IOPaths,
    cfg: AppConfig,
    debug: Boolean,
    persist: Boolean,
    goldData: Option[GoldData] = None
  ): Unit = {

    logger.info("=== Étape Spark ML ===")

    // Vérification/lecture des tables Gold
    val resolvedGold: GoldData =
      goldData.getOrElse {
        if (persist) {
          val goldJTExists = Readers.exists(spark, paths.goldJT)

          if (!goldJTExists) {
            logger.warn(
              "Tables Gold manquantes — lancement automatique de runGold()."
            )
            runGold(spark, paths, cfg, debug, persist)
          } else {
            // Reconstruction du chemin des targets comme dans runGold
            val goldBase =
              paths.goldJT.substring(0, paths.goldJT.lastIndexOf('/'))
            val targetsPath = s"$goldBase/targets"
            logger.info(
              "Lecture des tables GOLD (targetsPath) depuis Delta"
            )
            GoldData(
              Readers.readDelta(spark, targetsPath)
            )
          }
        } else {
          logger.info(
            "Persist désactivé — recalcul complet des tables Gold en mémoire."
          )
          runGold(spark, paths, cfg, debug, persist = false)
        }
      }

    // Configuration de base pour les features
    val baseCfg = FeatureConfig(
      labelCol = "is_pos",
      testFraction = 0.2,
      seed = 42L
    )

    // =======================
    // Liste des expériences
    // =======================
    val defaultTh = cfg.th.getOrElse(60)
    val experiments: Seq[ExperimentConfig] = Seq(
      // === Baseline (aucune météo)
      ExperimentConfig("D2", defaultTh, 0, 0, "Baseline_D2_th60_noWeather"),

      // === Étude 1 : impact du nombre d'heures météo ===
      // Origine seule
      ExperimentConfig("D2", defaultTh, 1, 0, "S1_origin_1h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 3, 0, "S1_origin_3h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 5, 0, "S1_origin_5h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 7, 0, "S1_origin_7h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 9, 0, "S1_origin_9h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 11, 0, "S1_origin_11h_D2_th60"),

      // Destination seule
      ExperimentConfig("D2", defaultTh, 0, 1, "S1_dest_1h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 0, 3, "S1_dest_3h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 0, 5, "S1_dest_5h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 0, 7, "S1_dest_7h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 0, 9, "S1_dest_9h_D2_th60"),
      ExperimentConfig("D2", defaultTh, 0, 11, "S1_dest_11h_D2_th60"),

      // Origine + destination
      ExperimentConfig("D2", defaultTh, 7, 7, "S1_origin7h_dest7h_D2_th60"),

      // === Étude 2 : variation du seuil th ===
      ExperimentConfig("D2", 15, 7, 7, "S2_D2_th15_origin7h_dest7h"),
      ExperimentConfig("D2", 30, 7, 7, "S2_D2_th30_origin7h_dest7h"),
      ExperimentConfig("D2", 45, 7, 7, "S2_D2_th45_origin7h_dest7h"),
      ExperimentConfig("D2", 60, 7, 7, "S2_D2_th60_origin7h_dest7h"),
      ExperimentConfig("D2", 90, 7, 7, "S2_D2_th90_origin7h_dest7h"),

      // === Étude 3 : variation du dataset ===
      ExperimentConfig("D1", defaultTh, 7, 7, "S3_D1_th60_origin7h_dest7h"),
      ExperimentConfig("D3", defaultTh, 7, 7, "S3_D3_th60_origin7h_dest7h"),
      ExperimentConfig("D4", defaultTh, 7, 7, "S3_D4_th60_origin7h_dest7h")
    )

    // =======================
    // Fonction utilitaire pour exécuter une expérience
    // =======================
    def runOneExperiment(e: ExperimentConfig): Unit = {
      logger.info(
        s"=== Expérience ${e.tag} : ds=${e.ds}, th=${e.th}, originHours=${e.originHours}, destHours=${e.destHours} ==="
      )

      val (trainDF, testDF, extraNumCols) =
        FeatureBuilder.prepareDataset(
          e.ds,
          e.th,
          baseCfg,
          e.originHours,
          e.destHours,
          raw = resolvedGold.targets
        )

      ModelingPipeline.trainAndEvaluate(
        spark,
        trainDF,
        testDF,
        e.ds,
        e.th,
        extraNumCols,
        e.tag
      )
    }

    // =======================
    // Exécution conditionnelle des expériences
    // =======================
    (cfg.ds, cfg.th, cfg.originHours, cfg.destHours, cfg.tag) match {
      // Cas 1 : un scénario spécifique a été passé en argument CLI
      case (Some(ds), Some(th), Some(origin), Some(dest), Some(tag)) =>
        val singleExp = ExperimentConfig(ds, th, origin, dest, tag)
        logger.info(s"Exécution ciblée d'une seule expérience : $singleExp")
        runOneExperiment(singleExp)

      // Cas 2 : aucun paramètre CLI => exécution de tous les scénarios
      case _ =>
        logger.info(
          "Aucun scénario spécifique fourni — exécution de toutes les expériences définies."
        )
        experiments.foreach(runOneExperiment)
    }

    logger.info("=== Étape Spark ML terminée ===")
  }

  // =======================
  // MAIN
  // =======================
  def main(args: Array[String]): Unit = {
    try {
      logger.info("Lancement de l'application Flight Delay Prediction...")

      // Chargement de la configuration de base (.conf)
      val baseCfg = AppConfig.load()

      // Parsing scopt complet
      val builder = OParser.builder[AppConfig]
      val parser = {
        import builder._
        OParser.sequence(
          programName("flight-delay-pipeline"),
          head("Flight Delay Prediction", "v1.0"),

          // ENVIRONNEMENT
          opt[String]("env")
            .action((x, c) => c.copy(env = Environment.fromString(x)))
            .text("Environnement d'exécution (Local, Hadoop, CI, etc.)"),
          opt[Unit]("debug")
            .action((_, c) => c.copy(debug = true))
            .text("Active le mode debug (analyses QA & sanity checks)"),
          opt[Unit]("persist")
            .action((_, c) => c.copy(persistOutputs = true))
            .text("Active l'écriture des tables (bronze/silver/gold)"),

          // INPUTS LOCAL
          opt[String]("deltaBase")
            .action((x, c) => c.copy(deltaBase = Some(x)))
            .text(
              "Base path des tables Delta (ex: /app/delta ou /app/delta-Exp)"
            ),
          opt[String]("inFlightsDir")
            .action((x, c) => c.copy(inFlightsDir = x))
            .text("Répertoire local des vols"),
          opt[String]("inWeatherDir")
            .action((x, c) => c.copy(inWeatherDir = x))
            .text("Répertoire local météo"),
          opt[String]("inMapping")
            .action((x, c) => c.copy(inMapping = x))
            .text("Fichier de mapping local"),

          // INPUTS HDFS
          opt[String]("hInFlightsDir")
            .action((x, c) => c.copy(hInFlightsDir = x))
            .text("Répertoire HDFS des vols"),
          opt[String]("hInWeatherDir")
            .action((x, c) => c.copy(hInWeatherDir = x))
            .text("Répertoire HDFS météo"),
          opt[String]("hInMapping")
            .action((x, c) => c.copy(hInMapping = x))
            .text("Mapping HDFS"),

          // OUTPUTS LOCAL
          opt[String]("analysisDir")
            .action((x, c) => c.copy(analysisDir = x))
            .text("Répertoire local Analysis"),
          opt[String]("deltaBronzeBase")
            .action((x, c) => c.copy(deltaBronzeBase = x))
            .text("Répertoire local Delta Bronze"),
          opt[String]("deltaSilverBase")
            .action((x, c) => c.copy(deltaSilverBase = x))
            .text("Répertoire local Delta Silver"),
          opt[String]("deltaGoldBase")
            .action((x, c) => c.copy(deltaGoldBase = x))
            .text("Répertoire local Delta Gold"),

          // OUTPUTS HDFS
          opt[String]("hanalysisDir")
            .action((x, c) => c.copy(hanalysisDir = x))
            .text("Répertoire HDFS Analysis"),
          opt[String]("hDeltaBronzeBase")
            .action((x, c) => c.copy(hDeltaBronzeBase = x))
            .text("Répertoire HDFS Delta Bronze"),
          opt[String]("hDeltaSilverBase")
            .action((x, c) => c.copy(hDeltaSilverBase = x))
            .text("Répertoire HDFS Delta Silver"),
          opt[String]("hDeltaGoldBase")
            .action((x, c) => c.copy(hDeltaGoldBase = x))
            .text("Répertoire HDFS Delta Gold"),

          // PARAMÈTRES
          opt[Seq[String]]("monthsF")
            .valueName("m1,m2,...")
            .action((x, c) => c.copy(monthsF = x))
            .text("Liste des mois vols (ex: 01,02,03)"),
          opt[Seq[String]]("monthsW")
            .valueName("m1,m2,...")
            .action((x, c) => c.copy(monthsW = x))
            .text("Liste des mois météo (ex: 01,02,03)"),
          opt[Int]("th")
            .action((x, c) => c.copy(th = Some(x)))
            .text("Seuil de retard en minutes (optionnel)"),
          opt[Double]("missingnessThreshold")
            .action((x, c) => c.copy(missingnessThreshold = x))
            .text("Seuil de valeurs manquantes (0–1)"),

          // SPARK
          opt[String]("sparkMaster")
            .action((x, c) => c.copy(sparkMaster = x))
            .text("Master Spark (local[*], yarn, k8s, etc.)"),
          opt[String]("sparkAppName")
            .action((x, c) => c.copy(sparkAppName = x))
            .text("Nom de l'application Spark"),
          opt[String]("sparkSqlExtensions")
            .action((x, c) => c.copy(sparkSqlExtensions = x))
            .text("Extensions Spark SQL"),
          opt[String]("sparkSqlCatalog")
            .action((x, c) => c.copy(sparkSqlCatalog = x))
            .text("Nom du catalogue Spark"),
          opt[Map[String, String]]("sparkConfs")
            .valueName("k1=v1,k2=v2,...")
            .action((x, c) => c.copy(sparkConfs = x))
            .text("Configurations Spark supplémentaires"),

          // MODÉLISATION
          opt[String]("stage")
            .action((x, c) => c.copy(stage = x.toLowerCase))
            .text(
              "Étape à exécuter : bronze, silver, gold, diagnostics, ml, all"
            ),
          opt[String]("ds")
            .action((x, c) => c.copy(ds = Some(x)))
            .text("Dataset (D1, D2, D3, D4)"),
          opt[Int]("originHours")
            .action((x, c) => c.copy(originHours = Some(x)))
            .text("Heures météo origine"),
          opt[Int]("destHours")
            .action((x, c) => c.copy(destHours = Some(x)))
            .text("Heures météo destination"),
          opt[String]("tag")
            .action((x, c) => c.copy(tag = Some(x)))
            .text("Tag unique de l'expérience"),
          help("help").text("Affiche cette aide et quitte")
        )
      }

      // Fusion de la config + arguments CLI
      OParser.parse(parser, args, baseCfg) match {
        case Some(cfg) =>
          AppConfig.logConfig(cfg)
          runPipeline(cfg)

        case None =>
          sys.exit(1)
      }

    } catch {
      case e: Exception =>
        logger.error("Échec de l'application", e)
        throw e
    }
  }

  // ============================================================
  // PIPELINE PRINCIPAL
  // ============================================================
  def runPipeline(cfg: AppConfig): Unit = {
    logger.info(s"Étape demandée : ${cfg.stage}")

    val spark = {
      val b = SparkSession
        .builder()
        .appName(cfg.sparkAppName)
        .master(cfg.sparkMaster)
        .config("spark.sql.extensions", cfg.sparkSqlExtensions)
        .config("spark.sql.catalog.spark_catalog", cfg.sparkSqlCatalog)

      // Log + application des configs utilisateur
      cfg.sparkConfs.foreach { case (k, v) =>
        logger.info(s"[SparkConfig] Applying $k = $v")
        b.config(k, v)
      }

      b.getOrCreate()
    }
    val progressLoggerEnabled =
      cfg.debug || spark.conf
        .getOption("spark.flight.progressLogger.enabled")
        .exists(_.toBoolean)
    if (progressLoggerEnabled) {
      registerSparkProgressLogger(spark)
    } else {
      logger.info(
        "[SparkStage] Progress logger disabled (enable with --debug or spark.flight.progressLogger.enabled=true)"
      )
    }

    val paths = PathResolver.resolve(cfg)
    val conf  = spark.sparkContext.getConf

    logger.info(s"[Paths] bronzeFlights=${paths.bronzeFlights}")
    logger.info(s"[Paths] silverFlights=${paths.silverFlights}")
    logger.info(s"[Paths] goldJT=${paths.goldJT}")
    conf.getAll.sorted.foreach { case (k, v) =>
      logger.info(s"[SparkConfig] $k = $v")
    }

    val debugMode   = cfg.debug
    val persistMode = cfg.persistOutputs

    cfg.stage.toLowerCase match {
      case "bronze" =>
        runBronze(spark, paths, debugMode, persistMode)
      case "silver" =>
        runSilver(spark, paths, debugMode, persistMode)
      case "gold" =>
        runGold(spark, paths, cfg, debugMode, persistMode)
      case "diagnostics" =>
        runDiagnostics(spark, paths, cfg, debugMode, persistMode)
      case "ml" =>
        runModeling(spark, paths, cfg, debugMode, persistMode)
      case "all" =>
        val bronzeData =
          runBronze(spark, paths, debugMode, persistMode)
        val silverData =
          runSilver(
            spark,
            paths,
            debugMode,
            persistMode,
            bronzeData = Some(bronzeData)
          )
        val goldData =
          runGold(
            spark,
            paths,
            cfg,
            debugMode,
            persistMode,
            silverData = Some(silverData)
          )
        runModeling(
          spark,
          paths,
          cfg,
          debugMode,
          persistMode,
          goldData = Some(goldData)
        )
      case other =>
        logger.error(
          s"Étape inconnue: $other (bronze, silver, gold, diagnostics, ml, all)"
        )
        sys.exit(1)
    }

    spark.stop()
    logger.info("Application terminée avec succès.")
  }

  private def registerSparkProgressLogger(spark: SparkSession): Unit =
    if (sparkListenerRegistered.compareAndSet(false, true)) {
      val stageMeta     = new ConcurrentHashMap[Int, (String, Int)]()
      val stageCounters = new ConcurrentHashMap[Int, AtomicInteger]()

      spark.sparkContext.addSparkListener(new SparkListener {
        override def onStageSubmitted(
          stageSubmitted: SparkListenerStageSubmitted
        ): Unit = {
          val info = stageSubmitted.stageInfo
          stageMeta.put(info.stageId, (info.name, info.numTasks))
          stageCounters.put(info.stageId, new AtomicInteger(0))
          logger.info(
            s"[SparkStage] Stage ${info.stageId} '${info.name}' started with ${info.numTasks} tasks"
          )
        }

        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
          Option(stageCounters.get(taskEnd.stageId)).foreach { counter =>
            val done  = counter.incrementAndGet()
            val total = stageMeta.getOrDefault(taskEnd.stageId, ("?", -1))._2
            logger.info(
              s"[SparkStage] Stage ${taskEnd.stageId} progress: $done/$total tasks — " +
                s"partition ${taskEnd.taskInfo.index} finished on ${taskEnd.taskInfo.host} in ${taskEnd.taskInfo.duration} ms"
            )
          }

        override def onStageCompleted(
          stageCompleted: SparkListenerStageCompleted
        ): Unit = {
          val info = stageCompleted.stageInfo
          val durationMs = (for {
            start <- info.submissionTime
            end   <- info.completionTime
          } yield end - start).getOrElse(-1L)
          logger.info(
            s"[SparkStage] Stage ${info.stageId} '${info.name}' completed in ${durationMs} ms"
          )
          stageCounters.remove(info.stageId)
          stageMeta.remove(info.stageId)
        }
      })
    }
}
