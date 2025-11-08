name := "flight"

version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.5.1" % "provided",
  "com.typesafe"     %  "config"     % "1.4.3",
  "org.scalatest"    %% "scalatest"  % "3.2.18" % Test
)

// Point d'entrée principal
Compile / mainClass := Some("com.emiasd.flight.Main")

// =======================================
// Configuration sbt-assembly
// =======================================
assembly / mainClass := Some("com.emiasd.flight.Main")
assembly / assemblyJarName := "flight-assembly.jar"

// Force la sortie uniquement dans target/scala-2.12/
assembly / assemblyOutputPath := baseDirectory.value / "target" / s"scala-${scalaBinaryVersion.value}" / "flight-assembly.jar"