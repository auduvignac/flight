// com/emiasd/flight/io/Writers.scala
package com.emiasd.flight.io

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object Writers {
  def writeDelta(
    df: DataFrame,
    path: String,
    partitions: Seq[String] = Nil,
    overwriteSchema: Boolean = false
  ): Unit = {

    val logger = Logger.getLogger(getClass.getName)

    // === Création du répertoire cible si nécessaire ===
    val dir = new File(path)
    if (!dir.exists()) {
      val created = dir.mkdirs()
      if (created)
        logger.info(s"Répertoire créé : ${dir.getAbsolutePath}")
      else
        logger.warn(
          s"Impossible de créer le répertoire : ${dir.getAbsolutePath}"
        )
    }

    // === Écriture Delta ===
    val w = df.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("overwriteSchema", overwriteSchema.toString)

    val finalW = if (partitions.nonEmpty) w.partitionBy(partitions: _*) else w

    logger.info(s"Écriture Delta à l'emplacement : $path")
    finalW.save(path)
    logger.info("Écriture terminée avec succès.")

  }
}
