// com/emiasd/flight/ml/ModelingPipeline.scala
package com.emiasd.flight.ml

import org.apache.log4j.Logger
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ModelingPipeline {

  val logger = Logger.getLogger(getClass.getName)

  final val LabelCol    = "label"
  final val FeaturesCol = "features"

  def buildRandomForestPipeline(extraNumCols: Array[String]): Pipeline = {

    val catCols: Array[String] =
      Array("carrier_id", "origin_id", "dest_id", "month")
    val baseNumCols: Array[String] = Array("dep_minutes", "year")
    val numCols: Array[String]     = baseNumCols ++ extraNumCols

    val indexers: Array[PipelineStage] = catCols.map { c =>
      new StringIndexer()
        .setInputCol(c)
        .setOutputCol(s"${c}_idx")
        .setHandleInvalid("keep")
    }

    val assembler = new VectorAssembler()
      .setInputCols(numCols ++ catCols.map(c => s"${c}_idx"))
      .setOutputCol(FeaturesCol)

    val rf = new RandomForestClassifier()
      .setLabelCol(LabelCol)
      .setFeaturesCol(FeaturesCol)
      .setNumTrees(200)
      .setMaxDepth(15)
      .setFeatureSubsetStrategy("sqrt")
      .setSubsamplingRate(1.0)
      .setMinInstancesPerNode(10)
      .setMaxBins(512) // nécessaire vu le nb de catégories
      .setSeed(42L)

    val stages: Array[PipelineStage] =
      indexers ++ Array[PipelineStage](assembler, rf)

    new Pipeline().setStages(stages)
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

    val pipeline: Pipeline   = buildRandomForestPipeline(extraNumCols)
    val model: PipelineModel = pipeline.fit(trainDF)

    // === Analyse des importances de features ===
    try {
      // On récupère le RF et le VectorAssembler depuis le pipeline
      val rfModel = model.stages.collect {
        case m: RandomForestClassificationModel => m
      }.head

      val assembler = model.stages.collect { case a: VectorAssembler => a }.head

      val inputCols = assembler.getInputCols

      import spark.implicits._

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

    // === FIN Analyse des importances de features ===

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
