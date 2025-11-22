package com.emiasd.flight.io

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Writers {

  /**
   * Écriture Delta unifiée pour Bronze, Silver et Gold.
   *
   * @param mode
   *   "standard" → Bronze/Silver (écriture simple) "gold" → OPTIMIZE + ZORDER
   *   activés
   * @param zorderCols
   *   Liste des colonnes ZORDER (utilisé seulement en mode gold)
   */
  def writeDelta(
    df: DataFrame,
    path: String,
    partitions: Seq[String] = Nil,
    overwriteSchema: Boolean = false,
    maxFiles: Int = 200,
    mode: String = "standard",    // "standard" ou "gold"
    zorderCols: Seq[String] = Nil // utilisé seulement pour gold
  )(spark: SparkSession): Unit = {

    val logger = Logger.getLogger(getClass.getName)
    val isGold = mode.toLowerCase == "gold"

    logger.info(s"[Writer][$mode] Initialisation de l'écriture Delta")

    // ==============================================================
    // 1. AUTO-COALESCE INTELLIGENT
    // ==============================================================
    val initialParts = df.rdd.getNumPartitions
    logger.info(s"[Writer][$mode] Partitions initiales DF = $initialParts")

    val dfOptim =
      if (initialParts > maxFiles) {
        logger.info(s"[Writer][$mode] Coalesce → $maxFiles partitions")
        df.coalesce(maxFiles)
      } else df

    // ==============================================================
    // 2. ÉCRITURE DELTA
    // ==============================================================
    val writerBase =
      dfOptim.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option("overwriteSchema", overwriteSchema.toString)

    val writer =
      if (partitions.nonEmpty) writerBase.partitionBy(partitions: _*)
      else writerBase

    logger.info(s"[Writer][$mode] Écriture Delta dans $path")
    writer.save(path)

    // ==============================================================
    // 3. MODE GOLD : OPTIMIZE + ZORDER
    // ==============================================================
    if (isGold) {
      try {
        logger.info(s"[Writer][gold] OPTIMIZE delta.`$path` (compactage)")
        spark.sql(s"OPTIMIZE delta.`$path`")

        if (zorderCols.nonEmpty) {
          val cols = zorderCols.mkString(", ")
          logger.info(s"[Writer][gold] ZORDER BY ($cols)")
          spark.sql(s"OPTIMIZE delta.`$path` ZORDER BY ($cols)")
        } else {
          logger.info("[Writer][gold] Aucun ZORDER (aucune colonne fournie).")
        }

      } catch {
        case e: Throwable =>
          logger.warn(
            s"[Writer][gold] OPTIMIZE/ZORDER non supporté ou erreur : ${e.getMessage}"
          )
      }

      logger.info("[Writer][gold] Optimisation GOLD terminée ✔")
    } else {
      logger.info(
        s"[Writer][standard] Aucune optimisation GOLD appliquée (mode standard)."
      )
    }

    logger.info(s"[Writer][$mode] Écriture Delta terminée ✔")
  }
}
