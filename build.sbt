
name :="shift_test"

scalaVersion :="2.10.4"

autoScalaLibrary := false

mainClass in assembly := Some("edu.ucsc.FauxShift")

version :="1.0"

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.3.1",
    "org.apache.spark" %% "spark-graphx" % "1.3.1",
    "org.scala-saddle" %% "saddle-core" % "1.3.+",
    "org.rogach" %% "scallop" % "0.9.5",
    "org.scala-lang" % "scala-library" % scalaVersion.value,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value
)


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "w3c", xs @ _*) => MergeStrategy.first
    case "about.html"     => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case "log4j.properties"     => MergeStrategy.concat
    case PathList("META-INF", "mailcap") => MergeStrategy.discard
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
