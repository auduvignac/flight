package com.emiasd.flight.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._ // Scala 2.12.x

final case class AppConfig(
  // === Environment ===
  env: Environment,

  // === Inputs (Local) ===
  deltaBase: Option[String] = None,
  inFlightsDir: String,
  inWeatherDir: String,
  inMapping: String,

  // === Inputs (Hadoop) ===
  hInFlightsDir: String,
  hInWeatherDir: String,
  hInMapping: String,

  // === Outputs (Local) ===
  deltaBronzeBase: String,
  deltaSilverBase: String,
  deltaGoldBase: String,

  // === Outputs (Hadoop) ===
  hDeltaBronzeBase: String,
  hDeltaSilverBase: String,
  hDeltaGoldBase: String,

  // === Processing params ===
  monthsF: Seq[String],
  monthsW: Seq[String],
  thMinutes: Int,
  missingnessThreshold: Double,

  // === Spark ===
  sparkMaster: String,
  sparkAppName: String,
  sparkSqlExtensions: String,
  sparkSqlCatalog: String,
  sparkConfs: Map[String, String],

  // === Exécution / Modélisation ===
  stage: String = "all",
  ds: Option[String] = None,
  originHours: Option[Int] = None,
  destHours: Option[Int] = None,
  tag: Option[String] = None
)

object AppConfig {

  val logger = Logger.getLogger(getClass.getName)

  private def envOf(s: String): Environment = s match {
    case "Hadoop" => Environment.Hadoop
    case _        => Environment.Local
  }

  private def getSeq(c: Config, path: String): Seq[String] =
    c.getStringList(path).asScala.toVector

  private def getSparkConfs(sparkCfg: Config): Map[String, String] = {
    val conf = sparkCfg.getConfig("conf")
    conf
      .entrySet()
      .asScala
      .map { e =>
        val k = e.getKey
        val v = conf.getString(k)
        k -> v
      }
      .toMap
  }

  def load(): AppConfig = {
    import java.io.File

    // Déterminer le chemin du fichier externe
    val configPathOpt =
      sys.props.get("app.config") orElse
        sys.env.get("APPLICATION_CONFIG_PATH") orElse
        sys.props.get("spark.app.config")

    // Chargement de la configuration externe si elle existe, sinon fallback sur ConfigFactory.load()
    val root = configPathOpt match {
      case Some(path) if new File(path).exists() =>
        logger.info(s"[AppConfig] Chargement du fichier externe : $path")
        ConfigFactory.parseFile(new File(path)).resolve()
      case Some(path) =>
        logger.info(
          s"[AppConfig] ⚠️ Fichier indiqué mais introuvable à $path — fallback sur le conf embarqué."
        )
        ConfigFactory.load()
      case None =>
        logger.info(
          "[AppConfig] Aucun chemin externe fourni — chargement du conf embarqué (application.conf du JAR)."
        )
        ConfigFactory.load()
    }

    // Validation
    if (!root.hasPath("app")) {
      throw new RuntimeException(
        "❌ Configuration invalide : clé 'app' manquante dans le fichier de configuration chargé."
      )
    }

    // Extraction des sections
    val app   = root.getConfig("app")
    val spark = root.getConfig("spark")

    AppConfig(
      env = envOf(app.getString("env")),
      // Local inputs
      inFlightsDir = app.getString("input.flights.dir"),
      inWeatherDir = app.getString("input.weather.dir"),
      inMapping = app.getString("input.mapping"),
      // Hadoop inputs
      hInFlightsDir = app.getConfig("hadoop").getString("input.flights.dir"),
      hInWeatherDir = app.getConfig("hadoop").getString("input.weather.dir"),
      hInMapping = app.getConfig("hadoop").getString("input.mapping"),
      // Local outputs
      deltaBronzeBase = app.getString("output.delta.base.bronze"),
      deltaSilverBase = app.getString("output.delta.base.silver"),
      deltaGoldBase = app.getString("output.delta.base.gold"),
      // Hadoop outputs
      hDeltaBronzeBase =
        app.getConfig("hadoop").getString("output.delta.base.bronze"),
      hDeltaSilverBase =
        app.getConfig("hadoop").getString("output.delta.base.silver"),
      hDeltaGoldBase =
        app.getConfig("hadoop").getString("output.delta.base.gold"),
      // Params
      monthsF = getSeq(app, "input.months_f"),
      monthsW = getSeq(app, "input.months_w"),
      thMinutes = app.getConfig("params").getInt("thMinutes"),
      missingnessThreshold =
        app.getConfig("params").getDouble("missingness.threshold"),
      // Spark
      sparkMaster = spark.getString("master"),
      sparkAppName = spark.getString("appName"),
      sparkSqlExtensions = spark.getString("sql.extensions"),
      sparkSqlCatalog = spark.getString("catalogClass"),
      sparkConfs = getSparkConfs(spark)
    )
  }

  /** Affiche la configuration chargée dans les logs. */
  def logConfig(cfg: AppConfig): Unit = {
    logger.info("========== Configuration Active ==========")
    logger.info(s"Environnement        : ${cfg.env}")
    logger.info(s"Étape demandée       : ${cfg.stage}")
    logger.info(s"Seuil retard (min)   : ${cfg.thMinutes}")
    logger.info(s"MissingnessThreshold : ${cfg.missingnessThreshold}")
    logger.info(s"Months Flights       : ${cfg.monthsF.mkString(", ")}")
    logger.info(s"Months Weather       : ${cfg.monthsW.mkString(", ")}")
    logger.info(s"Spark Master         : ${cfg.sparkMaster}")
    logger.info(s"Spark App Name       : ${cfg.sparkAppName}")
    // bases déclarées dans le conf
    logger.info(s"Delta Bronze Base    : ${cfg.deltaBronzeBase}")
    logger.info(s"Delta Silver Base    : ${cfg.deltaSilverBase}")
    logger.info(s"Delta Gold Base      : ${cfg.deltaGoldBase}")
    // base passée en CLI (optionnelle)
    logger.info(s"Delta Base (CLI opt) : ${cfg.deltaBase.getOrElse("-")}")
    logger.info(s"Dataset (opt)        : ${cfg.ds.getOrElse("-")}")
    logger.info(s"OriginHours (opt)    : ${cfg.originHours.getOrElse("-")}")
    logger.info(s"DestHours (opt)      : ${cfg.destHours.getOrElse("-")}")
    logger.info(s"Tag (opt)            : ${cfg.tag.getOrElse("-")}")
    logger.info("==========================================")
  }

}
