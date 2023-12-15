import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file(".")).
  settings(
    name := "dr2-ingest-parent-folder-opex-creator",
    resolvers += "s01-oss-sonatype-org-snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots",
    libraryDependencies ++= Seq(
      awsS3Client,
      fs2Core,
      log4jSlf4j,
      log4jCore,
      log4jTemplateJson,
      lambdaCore,
      lambdaJavaEvents,
      mockitoScala % Test,
      mockitoScalaTest % Test,
      pureConfigCats,
      pureConfig,
      reactorTest % Test,
      scalaTest % Test,
      scalaXml,
      upickle
    ),
    scalacOptions += "-deprecation"
  )
(assembly / assemblyJarName) := "dr2-ingest-parent-folder-opex-creator.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps@_*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _ => MergeStrategy.first
}

