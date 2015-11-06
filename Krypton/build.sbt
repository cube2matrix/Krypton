name := "Krypton"

version := "0.1"

scalaVersion := "2.10.5"


libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.4.1",
    "org.apache.spark" %% "spark-graphx" % "1.4.1",
    "org.scala-lang" % "scala-library" % "2.10.5",
    "org.scala-lang" % "scala-reflect" % "2.10.5",
    "org.scala-lang" % "scala-compiler" % "2.10.5",
    "org.scala-lang" % "scalap" % "2.10.5"
)