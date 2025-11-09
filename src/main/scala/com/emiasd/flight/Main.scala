package com.emiasd.flight

// =======================
// Imports
// =======================
import com.emiasd.flight.config.AppConfig
import com.emiasd.flight.spark.{PathResolver, SparkBuilder}
import org.apache.log4j.Logger

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
  def main(args: Array[String]): Unit =
    try {
      val logger = Logger.getLogger(getClass.getName)
      logger.info("🚀 Starting application...")

      val cfg   = AppConfig.load()
      val spark = SparkBuilder.build(cfg)

      val paths = PathResolver.resolve(cfg)
      logger.info(s"✅ IO paths resolved: $paths")

      logger.info("🏁 Application completed successfully.")
      spark.stop()
    } catch {
      case e: Exception =>
        logger.error("❌ Application failed", e)
        throw e
    }
}
