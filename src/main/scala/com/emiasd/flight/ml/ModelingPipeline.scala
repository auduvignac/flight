// src/main/scala/com/emiasd/flight/ml/ModelingPipeline.scala
package com.emiasd.flight.ml

import org.apache.log4j.Logger
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ModelingPipeline {

  val logger = Logger.getLogger(getClass.getName)

  final val LabelCol    = "label"
  final val FeaturesCol = "features"

  /**
   * Construit un Pipeline RandomForest "classique" :
   *   - StringIndexer sur carrier_id, origin_id, dest_id, month
   *   - VectorAssembler -> features
   *   - RandomForestClassifier
   */
  def buildRandomForestPipeline(): Pipeline = {

    val catCols: Array[String] =
      Array("carrier_id", "origin_id", "dest_id", "month")
    val numCols: Array[String] = Array("dep_minutes", "year")

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
      .setMaxBins(512)
      .setSeed(42L)

    // Typage explicite pour éviter l'erreur "overloaded method Array"
    val stages: Array[PipelineStage] =
      indexers ++ Array[PipelineStage](assembler, rf)

    new Pipeline().setStages(stages)
  }

  /**
   * Entraîne le pipeline sur trainDF, évalue sur testDF, et retourne le modèle
   * entraîné.
   */
  def trainAndEvaluate(
    spark: SparkSession,
    trainDF: DataFrame,
    testDF: DataFrame,
    ds: String,
    th: Int
  ): PipelineModel = {

    import spark.implicits._

    val pipeline: Pipeline = buildRandomForestPipeline()

    logger.info(
      s"[ModelingPipeline] Entraînement RandomForest (ds=$ds, th=$th)"
    )
    val model: PipelineModel = pipeline.fit(trainDF)

    logger.info(s"[ModelingPipeline] Évaluation sur test set (ds=$ds, th=$th)")
    val predictions: DataFrame = model.transform(testDF).cache()

    // === Metrics binaires ===
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

    // Accuracy
    val accuracy = predictions
      .withColumn("correct", expr("double(prediction = label)"))
      .agg(avg(col("correct")))
      .as[Double]
      .first()

    // Recall (sensibilité)
    val recall = predictions
      .filter(col(LabelCol) === 1.0)
      .withColumn("tp", expr("double(prediction = 1.0)"))
      .agg(avg(col("tp")))
      .as[Double]
      .first()

    // Precision
    val precision = predictions
      .filter(col("prediction") === 1.0)
      .withColumn("tp", expr("double(label = 1.0)"))
      .agg(avg(col("tp")))
      .as[Double]
      .first()

    logger.info(
      f"[ModelingPipeline] ds=$ds th=$th  ->  AUC ROC = $auc%.4f,  AUC PR = $prAuc%.4f"
    )
    logger.info(
      f"[ModelingPipeline] ds=$ds th=$th  ->  Accuracy = $accuracy%.4f, Recall = $recall%.4f, Precision = $precision%.4f"
    )

    predictions.unpersist()

    model
  }
}
