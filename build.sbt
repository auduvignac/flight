name := "flight"

version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  // Spark runtime : fournis par le cluster (pas embarqués dans le JAR)
  "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.5.1" % "provided",

  // Dépendance Delta intégrée dans ton assembly JAR
  "io.delta" %% "delta-spark" % "3.2.1",

  // Configuration Typesafe (application.conf)
  "com.typesafe" % "config" % "1.4.3",

  // Tests unitaires
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,

  // parsing des arguments
  "com.github.scopt" %% "scopt" % "4.1.0"
)

// =======================================
// Application
// =======================================
Compile / mainClass := Some("com.emiasd.flight.Main")

// =======================================
// Assembly configuration
// =======================================
assembly / mainClass := Some("com.emiasd.flight.Main")
assembly / assemblyJarName := "flight-assembly.jar"
assembly / assemblyOutputPath := baseDirectory.value / "target" / s"scala-${scalaBinaryVersion.value}" / "flight-assembly.jar"

// =======================================
// Scalafix + SemanticDB configuration
// =======================================
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-Ywarn-unused",
  "-Ywarn-unused-import"
)