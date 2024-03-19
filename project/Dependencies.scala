import sbt._
object Dependencies {
  private val mockitoScalaVersion = "1.17.30"
  lazy val pureConfigVersion = "0.17.6"
  private val log4CatsVersion = "2.6.0"

  lazy val awsS3Client = "uk.gov.nationalarchives" %% "da-s3-client" % "0.1.39"
  lazy val fs2Core = "co.fs2" %% "fs2-core" % "3.9.4"
  lazy val logbackVersion = "2.23.1"
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val log4CatsCore = "org.typelevel" %% "log4cats-core" % log4CatsVersion
  lazy val log4CatsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.3"
  lazy val lambdaJavaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.11.4"
  lazy val mockitoScala = "org.mockito" %% "mockito-scala" % mockitoScalaVersion
  lazy val mockitoScalaTest = "org.mockito" %% "mockito-scala-scalatest" % mockitoScalaVersion
  lazy val preservicaClient = "uk.gov.nationalarchives" %% "preservica-client-fs2" % "0.0.35"
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.18"
  lazy val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "2.2.0"
  lazy val upickle = "com.lihaoyi" %% "upickle" % "3.2.0"
  lazy val reactorTest = "io.projectreactor" % "reactor-test" % "3.6.2"
}
