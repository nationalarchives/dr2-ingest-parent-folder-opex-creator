package uk.gov.nationalarchives

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.Context
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils

import java.io.{ByteArrayInputStream, OutputStream}

class LambdaTest extends ExternalServicesTestUtils with MockitoSugar {
  val mockInput = s"""{"executionId":"9e32383f-52a7-4591-83dc-e3e598a6f1a7"}"""
  private val mockOutputStream = mock[OutputStream]
  private val mockContext = mock[Context]

  private def mockInputStream = new ByteArrayInputStream(mockInput.getBytes)

  "handleRequest" should "send an s3 'upload' request to upload Opex files with the correct content, file name and size" in {
    val commonPrefixes = List(
      "9e32383f-52a7-4591-83dc-e3e598a6f1a7/dir1/",
      "9e32383f-52a7-4591-83dc-e3e598a6f1a7/dir2/",
      "9e32383f-52a7-4591-83dc-e3e598a6f1a7/dir3/"
    )
    val sdkPublisher: IO[SdkPublisher[String]] = generateMockSdkPublisherWithPrefixes(commonPrefixes)
    val s3UploadResult: IO[CompletedUpload] = IO(
      CompletedUpload
        .builder()
        .response(PutObjectResponse.builder().build())
        .build()
    )

    val mockLambda = MockLambda(sdkPublisher, s3UploadResult)

    mockLambda.handleRequest(mockInputStream, mockOutputStream, mockContext)

    mockLambda.verifyInvocationsAndArgumentsPassed(
      List(
        "dir3",
        "dir2",
        "dir1"
      ),
      1
    )
  }

  "handleRequest" should "not send an s3 'upload' request to upload Opex files if no prefixes were returned" in {
    val commonPrefixes = Nil
    val sdkPublisher: IO[SdkPublisher[String]] = generateMockSdkPublisherWithPrefixes(commonPrefixes)
    val s3UploadResult: IO[CompletedUpload] = IO(
      CompletedUpload
        .builder()
        .response(PutObjectResponse.builder().build())
        .build()
    )

    val mockLambda = MockLambda(sdkPublisher, s3UploadResult)

    val thrownException = intercept[Exception] {
      mockLambda.handleRequest(mockInputStream, mockOutputStream, mockContext)
    }

    thrownException.getMessage should be("No uploads were attempted for 'opex/9e32383f-52a7-4591-83dc-e3e598a6f1a7/'")

    mockLambda.verifyInvocationsAndArgumentsPassed(numberOfUploads = 0)
  }

  "handleRequest" should "return an exception and not send an s3 'upload' request if 'listCommonPrefixes' returns an exception" in {
    val sdkPublisher: IO[SdkPublisher[String]] = IO.raiseError(new Exception("Bucket does not exist"))
    val s3UploadResult: IO[CompletedUpload] = IO(
      CompletedUpload
        .builder()
        .response(PutObjectResponse.builder().build())
        .build()
    )

    val mockLambda = MockLambda(sdkPublisher, s3UploadResult)

    val thrownException = intercept[Exception] {
      mockLambda.handleRequest(mockInputStream, mockOutputStream, mockContext)
    }

    thrownException.getMessage should be("Bucket does not exist")

    mockLambda.verifyInvocationsAndArgumentsPassed(numberOfUploads = 0)
  }

  "handleRequest" should "return an exception if an s3 'upload' attempt returns an exception" in {
    val commonPrefixes = List(
      "9e32383f-52a7-4591-83dc-e3e598a6f1a7/dir1/",
      "9e32383f-52a7-4591-83dc-e3e598a6f1a7/dir2/",
      "9e32383f-52a7-4591-83dc-e3e598a6f1a7/dir3/"
    )
    val sdkPublisher: IO[SdkPublisher[String]] = generateMockSdkPublisherWithPrefixes(commonPrefixes)
    val s3UploadResult: IO[CompletedUpload] = IO.raiseError(new Exception("Bucket does not exist"))

    val mockLambda = MockLambda(sdkPublisher, s3UploadResult)

    val thrownException = intercept[Exception] {
      mockLambda.handleRequest(mockInputStream, mockOutputStream, mockContext)
    }

    thrownException.getMessage should be("Bucket does not exist")

    mockLambda.verifyInvocationsAndArgumentsPassed(
      List(
        "dir3",
        "dir2",
        "dir1"
      ),
      numberOfUploads = 1
    )
  }
}
