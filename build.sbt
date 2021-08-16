
lazy val root = (project in file(".")).settings(
  name := projectName,
  version := artifactVersion,
  scalaVersion := "2.11.12",
  mainClass in(Compile, assembly) := Some(s"com.paytm.weather.$projectName")
).enablePlugins(AssemblyPlugin)
val artifactVersion = "1.0"
val projectName = "paytm-assignment"
val sparkVersion = "2.4.5"

resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.typesafeRepo("releases"),
  Resolver.sbtPluginRepo("releases"),
  Resolver.bintrayRepo("owner", "repo"),
  Resolver.jcenterRepo,
  Resolver.mavenCentral,
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile" withSources() withJavadoc(),
  "com.github.pureconfig" %% "pureconfig" % "0.11.1",
  "com.github.scopt" % "scopt_2.11" % "3.7.1",
  // Testing libraries
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

val meta = "META.INF(.)*".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

//build info
buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "com.paytm"
