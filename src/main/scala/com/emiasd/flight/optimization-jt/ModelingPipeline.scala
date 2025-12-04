// com/emiasd/flight/ml/ModelingPipeline.scala
package com.emiasd.flight.ml

import org.apache.log4j.Logger
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ModelingPipeline {

  val logger = Logger.getLogger(getClass.getName)

  final val LabelCol    = "label"
  final val FeaturesCol = "features"

  /**
   * Construit le pipeline RandomForest.
   *
   * @param extraNumCols
   *   liste des colonnes numériques météo à inclure (peut être vide)
   * @param ds
   *   identifiant du dataset (D1..D4)
   * @param th
   *   seuil de retard (15, 30, 45, 60, 90)
   */
  private def buildRandomForestPipeline(
    extraNumCols: Array[String],
    ds: String,
    th: Int
  ): Pipeline = {

    // 0) Contexte d'exécution
    val spark   = SparkSession.builder().getOrCreate()
    val master  = spark.sparkContext.master
    val isLocal = master.startsWith("local")

    // Slice le plus lourd de ton pipeline : D2_th15 avec 7h/7h => ~64k lignes + 300 features météo
    // On allège le RF uniquement dans ce cas-là en Local.
    val isBigLocalSlice =
      isLocal && ds == "D2" && th == 15 && extraNumCols.length >= 300

    // Hyperparamètres RF :
    //  - cas "gros slice local" : moins d'arbres / profondeur limitée,
    //    mais maxBins suffisamment grand pour supporter ~300 catégories (dest_id_idx).
    //  - cas standard : ta config "full".
    val (numTrees, maxDepth, maxBins, minInstancesPerNode) =
      if (isBigLocalSlice) {
        // ⚠ maxBins = 512 pour éviter l'erreur :
        // "DecisionTree requires maxBins (= 256) to be at least as large as the number of values..."
        (100, 12, 512, 20)
      } else {
        (200, 15, 512, 10)
      }

    // 1) Colonnes numériques "de base"
    val baseNumFeatures: Seq[String] = Seq(
      "dep_minutes",
      "year"
    )

    // 2) Colonnes numériques météo déjà produites par FeatureBuilder
    //    (o_h0_vis, o_vis_avg, d_temp_avg, ..., etc.)
    val weatherNumFeatures: Seq[String] =
      extraNumCols.toSeq

    val allNumFeatures: Seq[String] = baseNumFeatures ++ weatherNumFeatures

    // 3) Indexation des colonnes catégorielles
    val carrierIndexer = new StringIndexer()
      .setInputCol("carrier_id")
      .setOutputCol("carrier_id_idx")
      .setHandleInvalid("keep")

    val originIndexer = new StringIndexer()
      .setInputCol("origin_id")
      .setOutputCol("origin_id_idx")
      .setHandleInvalid("keep")

    val destIndexer = new StringIndexer()
      .setInputCol("dest_id")
      .setOutputCol("dest_id_idx")
      .setHandleInvalid("keep")

    val monthIndexer = new StringIndexer()
      .setInputCol("month") // <== on reste sur "month", pas "month_str"
      .setOutputCol("month_idx")
      .setHandleInvalid("keep")

    val catIdxCols: Seq[String] =
      Seq("carrier_id_idx", "origin_id_idx", "dest_id_idx", "month_idx")

    // 4) Assemblage des features
    val assembler = new VectorAssembler()
      .setInputCols((allNumFeatures ++ catIdxCols).toArray)
      .setOutputCol(FeaturesCol)

    // 5) Random Forest avec hyperparamètres adaptés
    val rf = new RandomForestClassifier()
      .setLabelCol(LabelCol)
      .setFeaturesCol(FeaturesCol)
      .setNumTrees(numTrees)
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .setMinInstancesPerNode(minInstancesPerNode)

    // 6) Pipeline complet
    new Pipeline()
      .setStages(
        Array(
          carrierIndexer,
          originIndexer,
          destIndexer,
          monthIndexer,
          assembler,
          rf
        )
      )
  }

  def trainAndEvaluate(
    spark: SparkSession,
    trainDF: DataFrame,
    testDF: DataFrame,
    ds: String,
    th: Int,
    extraNumCols: Array[String],
    tag: String
  ): PipelineModel = {

    import spark.implicits._

    logger.info(
      s"[ModelingPipeline] Entraînement RandomForest " +
        s"(ds=$ds, th=$th, tag=$tag, extraNumCols=${extraNumCols.length})"
    )

    val pipeline: Pipeline   = buildRandomForestPipeline(extraNumCols, ds, th)
    val model: PipelineModel = pipeline.fit(trainDF)

    // === Analyse des importances de features ===
    try {
      val rfModel = model.stages.collect {
        case m: RandomForestClassificationModel => m
      }.head

      val assembler = model.stages.collect { case a: VectorAssembler =>
        a
      }.head

      val inputCols: Array[String] = assembler.getInputCols

      val fiDF = inputCols
        .zip(rfModel.featureImportances.toArray)
        .toSeq
        .toDF("feature", "importance")
        .orderBy(desc("importance"))

      logger.info(s"[ModelingPipeline] Top 30 feature importances pour $tag :")
      fiDF.show(30, truncate = false)

    } catch {
      case e: Exception =>
        logger.warn(
          s"[ModelingPipeline] Impossible de calculer les importances pour $tag : ${e.getMessage}"
        )
    }

    // === Évaluation sur le test set ===
    logger.info(
      s"[ModelingPipeline] Évaluation sur test set (ds=$ds, th=$th, tag=$tag)"
    )

    val predictions: DataFrame = model.transform(testDF).cache()

    try {
      val evaluatorAuc = new BinaryClassificationEvaluator()
        .setLabelCol(LabelCol)
        .setRawPredictionCol("rawPrediction")
        .setMetricName("areaUnderROC")

      val evaluatorPrauc = new BinaryClassificationEvaluator()
        .setLabelCol(LabelCol)
        .setRawPredictionCol("rawPrediction")
        .setMetricName("areaUnderPR")

      val auc   = evaluatorAuc.evaluate(predictions)
      val prAuc = evaluatorPrauc.evaluate(predictions)

      val accuracy = predictions
        .withColumn("correct", expr("double(prediction = label)"))
        .agg(avg(col("correct")))
        .as[Double]
        .first()

      val recall = predictions
        .filter(col(LabelCol) === 1.0)
        .withColumn("tp", expr("double(prediction = 1.0)"))
        .agg(avg(col("tp")))
        .as[Double]
        .first()

      val precision = predictions
        .filter(col("prediction") === 1.0)
        .withColumn("tp", expr("double(label = 1.0)"))
        .agg(avg(col("tp")))
        .as[Double]
        .first()

      logger.info(
        f"[ModelingPipeline] [$tag] ds=$ds th=$th  ->  AUC ROC = $auc%.4f,  AUC PR = $prAuc%.4f"
      )
      logger.info(
        f"[ModelingPipeline] [$tag] ds=$ds th=$th  ->  Accuracy = $accuracy%.4f, Recall = $recall%.4f, Precision = $precision%.4f"
      )

    } finally
      // Toujours libérer la mémoire du cache, même si une exception survient
      predictions.unpersist(blocking = false)

    model
  }
}
