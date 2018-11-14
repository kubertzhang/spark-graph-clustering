name := "spark-graph-clustering"

version := "0.1"

libraryDependencies ++= Seq(
  // spark-core
  "org.apache.spark" %% "spark-core" % "2.2.0",
  // graphx
  "org.apache.spark" %% "spark-graphx" % "2.2.0",
  // Breeze
  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scalanlp" %% "breeze-viz" % "0.13.2"
)

resolvers ++= Seq(
  // Breeze
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

scalaVersion := "2.11.8"