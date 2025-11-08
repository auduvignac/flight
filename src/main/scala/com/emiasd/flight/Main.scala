package com.emiasd.flight

// =======================
// Imports
// =======================
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
  def main(args: Array[String]): Unit = {
    logger.info("Hello world")
  }
}