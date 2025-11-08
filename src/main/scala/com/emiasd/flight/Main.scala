package com.emiasd.flight

// =======================
// Imports
// =======================
import org.apache.log4j.Logger
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File

/**
 * Point d'entrée principal pour exécuter l'ensemble du pipeline
 */
object Main {
  // =======================
  // Logger
  // =======================
  implicit val logger: Logger = Logger.getLogger(getClass.getName)

  // =======================
  // Point d'entrée principal
  // =======================
  def main(args: Array[String]): Unit = {
    logger.info("Starting application...")

    // Charger la configuration depuis le fichier spécifié dans spark.app.config
    val config: Config = loadConfig()

    logger.info(s"Configuration loaded successfully")

    // Exemple d'utilisation de la configuration
    // val someValue = config.getString("path.to.property")
    // val someInt = config.getInt("path.to.number")

    logger.info("Application completed")
  }

  /**
   * Charge la configuration depuis le fichier spécifié par spark.app.config
   * ou utilise la configuration par défaut (application.conf)
   */
  private def loadConfig(): Config = {
    val configPath = sys.props.get("spark.app.config")

    configPath match {
      case Some(path) =>
        logger.info(s"Loading configuration from: $path")
        val configFile = new File(path)
        if (configFile.exists()) {
          ConfigFactory.parseFile(configFile).resolve()
        } else {
          logger.warn(s"Config file not found at $path, using default configuration")
          ConfigFactory.load()
        }
      case None =>
        logger.info("No spark.app.config specified, using default configuration")
        ConfigFactory.load()
    }
  }
}