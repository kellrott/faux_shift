
name :="shift_test"

scalaVersion :="2.10.4"

autoScalaLibrary := false

version :="1.0"

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.2.0",
    "org.apache.spark" %% "spark-graphx" % "1.2.0",
    "org.scala-saddle" %% "saddle-core" % "1.3.+",
    "org.rogach" %% "scallop" % "0.9.5",
    "org.scala-lang" % "scala-library" % scalaVersion.value,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value
)
