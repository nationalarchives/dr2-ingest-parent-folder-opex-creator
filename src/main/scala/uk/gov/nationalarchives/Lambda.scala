package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import fs2._
import fs2.interop.reactivestreams._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.Lambda.{Config, StepFnInput}
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}
import scala.io.Source
import scala.xml.PrettyPrinter

class Lambda extends RequestStreamHandler {
  val dAS3Client: DAS3Client[IO] = DAS3Client[IO]()
  implicit val inputReader: Reader[StepFnInput] = macroR[StepFnInput]

  def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = {
    val rawInput: String = Source.fromInputStream(input).mkString
    val stepFnInput = default.read[StepFnInput](rawInput)
    val keyPrefix = s"opex/${stepFnInput.executionId}/"
    val opexFileName = s"$keyPrefix${stepFnInput.executionId}.opex"

    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      publisher <- dAS3Client.listCommonPrefixes(config.stagingCacheBucket, keyPrefix)
      completedUpload <- publisher
        .toStreamBuffered[IO](1024 * 5)
        .through(accumulatePrefixes)
        .map(generateOpexWithManifest)
        .flatMap { opexXmlString => uploadToS3(opexXmlString, opexFileName, config.stagingCacheBucket) }
        .compile
        .toList
      verifiedCompletedUpload <-
        if (completedUpload.nonEmpty) IO(completedUpload)
        else
          IO.raiseError(
            new Exception(s"No uploads were attempted for '$keyPrefix'")
          )
    } yield verifiedCompletedUpload
  }.unsafeRunSync()

  private def accumulatePrefixes(s: fs2.Stream[IO, String]): fs2.Stream[IO, List[String]] =
    s.fold[List[String]](Nil) { case (acc, path) =>
      path :: acc
    }.filter(_.nonEmpty)

  private def generateOpexWithManifest(paths: List[String]): String = {
    val folderElems = paths.map { path => <opex:Folder>{path.split("/").last}</opex:Folder> }
    val opex =
      <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
        <opex:Transfer>
          <opex:Manifest>
            <opex:Folders>
              {folderElems}
            </opex:Folders>
          </opex:Manifest>
        </opex:Transfer>
      </opex:OPEXMetadata>
    new PrettyPrinter(80, 2).format(opex)
  }

  private def uploadToS3(
      opexXmlContent: String,
      fileName: String,
      bucketName: String
  ): Stream[IO, CompletedUpload] = Stream.eval {
    Stream
      .emits[IO, Byte](opexXmlContent.getBytes)
      .chunks
      .map(_.toByteBuffer)
      .toUnicastPublisher
      .use { publisher => dAS3Client.upload(bucketName, fileName, opexXmlContent.getBytes.length, publisher) }
  }
}

object Lambda extends App {
  case class StepFnInput(executionId: String)
  private case class Config(stagingCacheBucket: String)
}
