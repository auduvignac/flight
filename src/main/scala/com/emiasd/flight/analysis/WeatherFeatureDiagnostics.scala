// com/emiasd/flight/analysis/WeatherFeatureDiagnostics.scala
package com.emiasd.flight.analysis

import com.emiasd.flight.ml.FeatureBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Diagnostic tool to identify why weather features have zero importance.
 *
 * This script performs 4 key analyses:
 *   1. Array population (Wo/Wd emptiness) 2. Struct content quality (NULL
 *      fields) 3. Feature variance after extraction 4. Weather source data
 *      coverage
 */
object WeatherFeatureDiagnostics {

  val logger: Logger = Logger.getLogger(getClass.getName)

  /**
   *   1. Check array population statistics
   */
  def checkArrayPopulation(df: DataFrame): Unit = {
    logger.info("=" * 80)
    logger.info("1. ARRAY POPULATION ANALYSIS")
    logger.info("=" * 80)

    val total = df.count()
    logger.info(s"Total flights: $total")

    // Basic array size statistics
    val arraySizes = df.select(
      size(col("Wo")).as("nWo"),
      size(col("Wd")).as("nWd")
    )

    logger.info("\n--- Array Size Distribution ---")
    arraySizes.describe().show(truncate = false)

    // Empty array counts
    val emptyCounts = df
      .agg(
        sum(when(size(col("Wo")) === 0, 1).otherwise(0)).as("empty_Wo"),
        sum(when(size(col("Wd")) === 0, 1).otherwise(0)).as("empty_Wd"),
        count(lit(1)).as("total")
      )
      .collect()(0)

    val emptyWo = emptyCounts.getAs[Long]("empty_Wo")
    val emptyWd = emptyCounts.getAs[Long]("empty_Wd")

    logger.info(
      s"\nEmpty Wo arrays: $emptyWo / $total (${100.0 * emptyWo / total}%.2f%%)"
    )
    logger.info(
      s"Empty Wd arrays: $emptyWd / $total (${100.0 * emptyWd / total}%.2f%%)"
    )

    // Distribution of array sizes (histogram)
    logger.info("\n--- Wo Array Size Histogram ---")
    df.groupBy(size(col("Wo")).as("wo_size"))
      .count()
      .orderBy("wo_size")
      .show(20, truncate = false)

    logger.info("\n--- Wd Array Size Histogram ---")
    df.groupBy(size(col("Wd")).as("wd_size"))
      .count()
      .orderBy("wd_size")
      .show(20, truncate = false)

    // Flights with both arrays non-empty
    val bothNonEmpty = df
      .filter(
        size(col("Wo")) > 0 && size(col("Wd")) > 0
      )
      .count()

    logger.info(
      s"\nFlights with BOTH Wo and Wd non-empty: $bothNonEmpty / $total (${100.0 * bothNonEmpty / total}%.2f%%)"
    )
  }

  /**
   * 2. Check struct content quality (NULL fields)
   */
  def checkStructContent(df: DataFrame): Unit = {
    logger.info("\n" + "=" * 80)
    logger.info("2. STRUCT CONTENT ANALYSIS")
    logger.info("=" * 80)

    // Filter to flights with non-empty arrays
    val withArrays = df.filter(size(col("Wo")) > 0 || size(col("Wd")) > 0)
    val n          = withArrays.count()

    logger.info(s"\nAnalyzing $n flights with non-empty weather arrays")

    // Weather fields to check
    val weatherFields = Seq(
      "vis",
      "tempC",
      "dewC",
      "rh",
      "windKt",
      "windDir",
      "altim",
      "slp",
      "stnp",
      "precip",
      "wxType",
      "sky"
    )

    // Check NULL rates for first element in Wo
    logger.info("\n--- NULL Rates in Wo[0] (first hourly observation) ---")
    val woNulls = weatherFields.map { field =>
      val nullCount = withArrays
        .filter(size(col("Wo")) > 0)
        .select(col("Wo").getItem(0).getField(s"o_$field").isNull.as("is_null"))
        .filter(col("is_null"))
        .count()

      (field, nullCount)
    }

    woNulls.sortBy(-_._2).foreach { case (field, count) =>
      logger.info(
        f"  o_$field%-15s : $count%8d nulls (${100.0 * count / n}%6.2f%%)"
      )
    }

    // Check NULL rates for first element in Wd
    logger.info("\n--- NULL Rates in Wd[0] (first hourly observation) ---")
    val wdNulls = weatherFields.map { field =>
      val nullCount = withArrays
        .filter(size(col("Wd")) > 0)
        .select(col("Wd").getItem(0).getField(s"d_$field").isNull.as("is_null"))
        .filter(col("is_null"))
        .count()

      (field, nullCount)
    }

    wdNulls.sortBy(-_._2).foreach { case (field, count) =>
      logger.info(
        f"  d_$field%-15s : $count%8d nulls (${100.0 * count / n}%6.2f%%)"
      )
    }

    // Check for completely NULL structs (all fields NULL)
    logger.info("\n--- Checking for completely NULL structs ---")

    val woCompletelyNull = withArrays
      .filter(size(col("Wo")) > 0)
      .select(
        weatherFields
          .map(f => col("Wo").getItem(0).getField(s"o_$f").isNull)
          .reduce(_ && _)
          .as("all_null")
      )
      .filter(col("all_null"))
      .count()

    val wdCompletelyNull = withArrays
      .filter(size(col("Wd")) > 0)
      .select(
        weatherFields
          .map(f => col("Wd").getItem(0).getField(s"d_$f").isNull)
          .reduce(_ && _)
          .as("all_null")
      )
      .filter(col("all_null"))
      .count()

    logger.info(
      s"Wo[0] completely NULL: $woCompletelyNull / $n (${100.0 * woCompletelyNull / n}%.2f%%)"
    )
    logger.info(
      s"Wd[0] completely NULL: $wdCompletelyNull / $n (${100.0 * wdCompletelyNull / n}%.2f%%)"
    )

    // Sample some actual struct values
    logger.info("\n--- Sample Wo[0] structs (first 5 non-empty) ---")
    withArrays
      .filter(size(col("Wo")) > 0)
      .select(
        col("F.flight_key"),
        col("Wo").getItem(0).as("wo_first")
      )
      .show(5, truncate = false)

    logger.info("\n--- Sample Wd[0] structs (first 5 non-empty) ---")
    withArrays
      .filter(size(col("Wd")) > 0)
      .select(
        col("F.flight_key"),
        col("Wd").getItem(0).as("wd_first")
      )
      .show(5, truncate = false)
  }

  /**
   * 3. Check feature variance after extraction
   */
  def checkFeatureVariance(
    spark: SparkSession,
    df: DataFrame,
    originHours: Int,
    destHours: Int
  ): Unit = {
    logger.info("\n" + "=" * 80)
    logger.info("3. FEATURE VARIANCE ANALYSIS")
    logger.info("=" * 80)

    logger.info(
      s"\nExtracting features with originHours=$originHours, destHours=$destHours"
    )

    // Build base features (without weather)
    val cfg = FeatureBuilder.FeatureConfig(
      labelCol = "is_pos",
      testFraction = 0.2,
      seed = 42L
    )
    val baseFeatures = FeatureBuilder.buildFlatFeatures(df, cfg)

    // Add weather features
    val withWeather = if (originHours > 0 || destHours > 0) {
      FeatureBuilder.addWeatherFeatures(baseFeatures, originHours, destHours)
    } else {
      baseFeatures
    }

    val weatherCols = FeatureBuilder.weatherFeatureNames(originHours, destHours)

    logger.info(s"\nTotal weather features: ${weatherCols.length}")

    if (weatherCols.isEmpty) {
      logger.warn(
        "No weather features to analyze (originHours=0 and destHours=0)"
      )
    } else {
      // Compute variance for all weather features
      logger.info("\n--- Feature Variance Statistics ---")

      val varianceExprs = weatherCols.map { colName =>
        variance(col(colName)).as(s"var_$colName")
      }

      val variances = withWeather
        .agg(
          varianceExprs.head,
          varianceExprs.tail: _*
        )
        .collect()(0)

      // Analyze variance distribution
      val varianceMap = weatherCols.map { colName =>
        val v = Option(variances.getAs[Double](s"var_$colName")).getOrElse(0.0)
        (colName, v)
      }.sortBy(-_._2)

      // Count zero-variance features
      val zeroVariance    = varianceMap.count(_._2 == 0.0)
      val nonZeroVariance = varianceMap.count(_._2 > 0.0)

      logger.info(
        f"\nZero variance features: $zeroVariance / ${weatherCols.length}"
      )
      logger.info(
        f"Non-zero variance features: $nonZeroVariance / ${weatherCols.length}"
      )

      // Show top features by variance
      logger.info("\n--- Top 20 Features by Variance ---")
      varianceMap.take(20).foreach { case (name, variance) =>
        logger.info(f"  $name%-30s : variance = $variance%.6e")
      }

      // Show bottom features (zero or near-zero variance)
      logger.info(
        "\n--- Bottom 20 Features by Variance (likely problematic) ---"
      )
      varianceMap.takeRight(20).reverse.foreach { case (name, variance) =>
        logger.info(f"  $name%-30s : variance = $variance%.6e")
      }

      // Check mean and stddev for a few key features
      logger.info("\n--- Statistics for Key Weather Features ---")
      val keyFeatures = Seq(
        "o_vis_avg",
        "o_temp_avg",
        "o_wind_avg",
        "o_precip_sum",
        "d_vis_avg",
        "d_temp_avg",
        "d_wind_avg",
        "d_precip_sum"
      )
        .filter(weatherCols.contains)

      if (keyFeatures.nonEmpty) {
        val statsExprs = keyFeatures.flatMap { colName =>
          Seq(
            mean(col(colName)).as(s"mean_$colName"),
            stddev(col(colName)).as(s"std_$colName"),
            min(col(colName)).as(s"min_$colName"),
            max(col(colName)).as(s"max_$colName")
          )
        }

        val stats =
          withWeather.agg(statsExprs.head, statsExprs.tail: _*).collect()(0)

        keyFeatures.foreach { colName =>
          val meanVal =
            Option(stats.getAs[Double](s"mean_$colName")).getOrElse(0.0)
          val stdVal =
            Option(stats.getAs[Double](s"std_$colName")).getOrElse(0.0)
          val minVal =
            Option(stats.getAs[Double](s"min_$colName")).getOrElse(0.0)
          val maxVal =
            Option(stats.getAs[Double](s"max_$colName")).getOrElse(0.0)

          logger.info(
            f"  $colName%-20s : mean=$meanVal%8.2f, std=$stdVal%8.2f, min=$minVal%8.2f, max=$maxVal%8.2f"
          )
        }
      }

      // Sample actual feature values
      logger.info("\n--- Sample Feature Values (first 10 rows) ---")
      withWeather
        .select((Seq("flight_key", "label") ++ keyFeatures).map(col): _*)
        .show(10, truncate = false)
    }
  }

  /**
   * 4. Check Bronze weather data quality
   */
  def checkBronzeWeatherQuality(
    spark: SparkSession,
    bronzeWeatherPath: String
  ): Unit = {
    logger.info("\n" + "=" * 80)
    logger.info("4. BRONZE WEATHER DATA QUALITY")
    logger.info("=" * 80)

    BronzeWeatherCheck.checkBronzeWeather(spark, bronzeWeatherPath)
  }

  /**
   * 5. Check weather source data coverage
   */
  def checkWeatherSourceCoverage(
    spark: SparkSession,
    goldJT: DataFrame,
    silverWeatherPath: String,
    silverFlightsPath: String
  ): Unit = {
    logger.info("\n" + "=" * 80)
    logger.info("4. WEATHER SOURCE DATA COVERAGE")
    logger.info("=" * 80)

    // Load source data
    val weatherDF = spark.read.format("delta").load(silverWeatherPath)
    val flightsDF = spark.read.format("delta").load(silverFlightsPath)

    // Count unique airports
    val weatherAirports = weatherDF.select(col("airport_id")).distinct().count()
    val flightOrigins =
      flightsDF.select(col("origin_airport_id")).distinct().count()
    val flightDests =
      flightsDF.select(col("dest_airport_id")).distinct().count()
    val flightAirports = flightsDF
      .select(col("origin_airport_id").as("airport_id"))
      .union(flightsDF.select(col("dest_airport_id").as("airport_id")))
      .distinct()
      .count()

    logger.info(s"\n--- Airport Coverage ---")
    logger.info(s"Unique airports in weather data: $weatherAirports")
    logger.info(s"Unique origin airports in flights: $flightOrigins")
    logger.info(s"Unique destination airports in flights: $flightDests")
    logger.info(s"Total unique airports in flights: $flightAirports")

    // Check overlap
    val flightAirportSet = flightsDF
      .select(col("origin_airport_id").as("airport_id"))
      .union(flightsDF.select(col("dest_airport_id").as("airport_id")))
      .distinct()

    val weatherAirportSet = weatherDF.select(col("airport_id")).distinct()

    val inBoth = flightAirportSet.join(weatherAirportSet, "airport_id").count()
    val inFlightOnly = flightAirportSet
      .join(weatherAirportSet, Seq("airport_id"), "left_anti")
      .count()
    val inWeatherOnly = weatherAirportSet
      .join(flightAirportSet, Seq("airport_id"), "left_anti")
      .count()

    logger.info(s"\nAirports in BOTH flights and weather: $inBoth")
    logger.info(
      s"Airports in flights but NOT in weather: $inFlightOnly (${100.0 * inFlightOnly / flightAirports}%.2f%%)"
    )
    logger.info(s"Airports in weather but NOT in flights: $inWeatherOnly")

    // Show airports missing weather data
    if (inFlightOnly > 0) {
      logger.info("\n--- Sample Airports Missing Weather Data ---")
      flightAirportSet
        .join(weatherAirportSet, Seq("airport_id"), "left_anti")
        .show(20, truncate = false)
    }

    // Temporal coverage
    logger.info("\n--- Temporal Coverage ---")
    val weatherTemporal = weatherDF
      .agg(
        min(col("obs_utc")).as("weather_min_ts"),
        max(col("obs_utc")).as("weather_max_ts"),
        count(lit(1)).as("weather_obs_count")
      )
      .collect()(0)

    val flightTemporal = flightsDF
      .agg(
        min(col("dep_ts_utc")).as("flight_min_ts"),
        max(col("dep_ts_utc")).as("flight_max_ts"),
        count(lit(1)).as("flight_count")
      )
      .collect()(0)

    logger.info(
      s"Weather observations: ${weatherTemporal.getAs[Long]("weather_obs_count")}"
    )
    logger.info(
      s"  Time range: ${weatherTemporal.getAs[Any]("weather_min_ts")} to ${weatherTemporal
          .getAs[Any]("weather_max_ts")}"
    )

    logger.info(s"\nFlights: ${flightTemporal.getAs[Long]("flight_count")}")
    logger.info(
      s"  Time range: ${flightTemporal.getAs[Any]("flight_min_ts")} to ${flightTemporal
          .getAs[Any]("flight_max_ts")}"
    )

    // Observations per airport
    logger.info("\n--- Weather Observations per Airport (sample) ---")
    weatherDF
      .groupBy(col("airport_id"))
      .agg(count(lit(1)).as("obs_count"))
      .orderBy(desc("obs_count"))
      .show(20, truncate = false)
  }

  /**
   * Main diagnostic function - runs all checks
   */
  def runDiagnostics(
    spark: SparkSession,
    goldJTPath: String,
    silverWeatherPath: String,
    silverFlightsPath: String,
    bronzeWeatherPath: String,
    ds: String = "D2",
    th: Int = 60,
    originHours: Int = 7,
    destHours: Int = 7
  ): Unit = {

    logger.info("=" * 80)
    logger.info("WEATHER FEATURE DIAGNOSTICS")
    logger.info("=" * 80)
    logger.info(s"Dataset: $ds, Threshold: $th minutes")
    logger.info(
      s"Feature window: originHours=$originHours, destHours=$destHours"
    )

    // Load Gold table
    val targetsPath =
      goldJTPath.substring(0, goldJTPath.lastIndexOf('/')) + "/targets"
    logger.info(s"\nLoading targets from: $targetsPath")

    val targetsDF = spark.read
      .format("delta")
      .load(targetsPath)
      .filter(col("ds") === lit(ds) && col("th") === lit(th))

    val n = targetsDF.count()
    logger.info(s"Loaded $n rows for ds=$ds, th=$th")

    if (n == 0) {
      logger.error(s"No data found for ds=$ds, th=$th. Aborting diagnostics.")
    } else {
      // Run all diagnostic checks
      checkArrayPopulation(targetsDF)
      checkStructContent(targetsDF)
      checkFeatureVariance(spark, targetsDF, originHours, destHours)

      // Check Bronze weather data quality (before Silver transformations)
      checkBronzeWeatherQuality(spark, bronzeWeatherPath)

      checkWeatherSourceCoverage(
        spark,
        targetsDF,
        silverWeatherPath,
        silverFlightsPath
      )

      logger.info("\n" + "=" * 80)
      logger.info("DIAGNOSTICS COMPLETE")
      logger.info("=" * 80)
    }
  }
}
