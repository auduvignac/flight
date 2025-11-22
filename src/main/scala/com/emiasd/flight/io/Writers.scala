package com.emiasd.flight.io

import io.delta.tables._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession} // Delta Lake API

object Writers {

  /**
   * Écriture Delta optimisée + auto-coalesce + OPTIMIZE + ZORDER.
   *
   * @param df
   *   DataFrame à écrire
   * @param path
   *   Emplacement Delta Lake
   * @param partitions
   *   Colonnes de partitionnement
   * @param overwriteSchema
   *   Écraser le schéma si nécessaire
   * @param maxFiles
   *   Nombre maximum de fichiers parquet
   * @param zorderCols
   *   Colonnes pour ZORDER (optionnel)
   */
  def writeDelta(
    df: DataFrame,
    path: String,
    partitions: Seq[String] = Nil,
    overwriteSchema: Boolean = false,
    maxFiles: Int = 200,
    zorderCols: Seq[String] = Nil
  )(spark: SparkSession): Unit = {

    val logger = Logger.getLogger(getClass.getName)

    val initialParts = df.rdd.getNumPartitions
    logger.info(s"[Writers] Partitions initiales : $initialParts")

    // ======================================================
    // AUTO-COALESCE INTELLIGENT
    // ======================================================
    val dfOptimized =
      if (initialParts > maxFiles) {
        logger.info(
          s"[Writers] Coalesce → $maxFiles partitions (au lieu de $initialParts)"
        )
        df.coalesce(maxFiles)
      } else {
        logger.info("[Writers] Coalesce non nécessaire.")
        df
      }

    // ======================================================
    // ÉCRITURE DELTA
    // ======================================================
    val writerBase =
      dfOptimized.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option("overwriteSchema", overwriteSchema.toString)

    val writer =
      if (partitions.nonEmpty) {
        logger.info(
          s"[Writers] Partitionnement Delta : ${partitions.mkString(", ")}"
        )
        writerBase.partitionBy(partitions: _*)
      } else writerBase

    logger.info(s"[Writers] Écriture Delta → $path")
    writer.save(path)

    // ======================================================
    // OPTIMIZE + ZORDER
    // ======================================================
    try {
      logger.info(s"[Writers] OPTIMIZE (Delta Lake)...")

      val deltaTable = DeltaTable.forPath(spark, path)

      // ---- OPTIMIZE ----
      spark.sql(s"OPTIMIZE delta.`$path`")

      // ---- ZORDER ----
      if (zorderCols.nonEmpty) {
        val cols = zorderCols.mkString(", ")
        logger.info(s"[Writers] ZORDER BY $cols")
        spark.sql(s"OPTIMIZE delta.`$path` ZORDER BY ($cols)")
      } else {
        logger.info("[Writers] ZORDER désactivé (aucune colonne fournie).")
      }

      logger.info("[Writers] OPTIMIZE + ZORDER terminés ✔")

    } catch {
      case e: Throwable =>
        logger.warn(
          s"[Writers] OPTIMIZE/ZORDER non supporté ou erreur ignorée : ${e.getMessage}"
        )
    }
  }
}
