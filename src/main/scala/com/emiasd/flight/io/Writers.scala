// com/emiasd/flight/io/Writers.scala
package com.emiasd.flight.io

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.AccessControlException
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.IOException

object Writers {

  /**
   * Création universelle et robuste du répertoire (local ou HDFS/CephFS/S3) en
   * utilisant l'API Hadoop FileSystem, avec diagnostic d'erreur amélioré.
   * Fonctionne dans TOUTES les configurations Spark (local et cluster).
   */
  def mkdirSmart(
    spark: SparkSession,
    path: String
  )(logger: Logger): Unit = {

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val p          = new Path(path)

    try {
      // Récupère l'implémentation de FileSystem appropriée basée sur le chemin (URI).
      val fs     = FileSystem.get(new java.net.URI(path), hadoopConf)
      val fsType = fs.getClass.getSimpleName

      // Vérification et Création
      if (!fs.exists(p)) {
        logger.info(
          s"[mkdirSmart] Tentative de création du répertoire ($fsType) : $path"
        )

        if (fs.mkdirs(p)) {
          logger.info(
            s"[mkdirSmart] ✅ Répertoire créé avec succès ($fsType) : $path"
          )
        } else {
          // L'appel fs.mkdirs peut retourner false sans exception
          logger.warn(
            s"[mkdirSmart] ⚠️ Impossible de créer le répertoire (fs.mkdirs a retourné false) : $path"
          )
        }
      } else {
        logger.info(s"[mkdirSmart] Le répertoire existe déjà : $path")
      }

    } catch {
      case e: AccessControlException =>
        logger.error(
          s"[mkdirSmart] ❌ ERREUR DE PERMISSION (ACL) lors de la création de : $path",
          e
        )
        logger.error(
          s"Vérifiez les droits d'écriture de l'utilisateur Spark sur le chemin : $path."
        )

      case e: IOException =>
        logger.error(
          s"[mkdirSmart] ❌ ERREUR I/O générique lors de la création de : $path",
          e
        )
        logger.error(
          "Ceci peut indiquer un problème réseau, de configuration du FS, ou un chemin invalide."
        )

      case e: Exception =>
        logger.error(
          s"[mkdirSmart] ❌ ERREUR INCONNUE lors de la création de : $path",
          e
        )
    }
  }

  /**
   * Écriture Delta générique robuste avec gestion des erreurs de transaction.
   */
  def writeDelta(
    df: DataFrame,
    path: String,
    partitions: Seq[String] = Nil,
    overwriteSchema: Boolean = false
  ): Unit = {

    val spark: SparkSession     = df.sparkSession
    implicit val logger: Logger = Logger.getLogger(getClass.getName)

    // === Création du répertoire cible de manière générique et sécurisée ===
    mkdirSmart(spark, path)(logger)

    // === Préparation du Writer Delta ===
    val w = df.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("overwriteSchema", overwriteSchema.toString)

    val finalW =
      if (partitions.nonEmpty) w.partitionBy(partitions: _*)
      else w

    // === Bloc TRY-CATCH pour la résilience de l'écriture ===
    try {
      logger.info(s"Écriture Delta à l'emplacement : $path")

      // Opération critique : L'écriture
      finalW.save(path)

      logger.info("✅ Écriture terminée avec succès.")

    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        // Erreur courante de Delta, par exemple: partitionnement incorrect, schéma incompatible
        logger.error(
          s"❌ ERREUR D'ANALYSE (Delta/Spark) : Vérifiez le schéma ou le partitionnement pour $path.",
          e
        )
        throw new RuntimeException(
          s"Échec de l'écriture Delta (Analyse) : $path",
          e
        )

      case e: Exception =>
        // Capture toute autre exception (I/O, réseau, etc.)
        logger.error(
          s"❌ Échec CRITIQUE de l'écriture Delta à l'emplacement : $path",
          e
        )
        // On relance l'exception pour que le job Spark s'arrête
        throw new RuntimeException(
          s"Impossible d'écrire la table Delta à $path",
          e
        )
    }
  }
}
