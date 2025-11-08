name := "flight"

version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql"  % "3.5.1",
  "org.scalatest"    %% "scalatest"  % "3.2.18" % Test
)

Compile / mainClass := Some("com.emiasd.flight.Main")
