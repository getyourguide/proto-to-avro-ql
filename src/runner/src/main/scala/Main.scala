import TimingUtils.time
import com.getyourguide.proto.to.avro.Schemas.SchemaOps
import com.getyourguide.proto.to.avro.UserDefinedMappings.{Constant, Message}
import com.getyourguide.proto.to.avro.{ProtoToAvroTranslator, Schemas, UserDefinedMappings}
import com.google.ads.googleads.lib.GoogleAdsClient
import com.google.ads.googleads.v7.services.{GoogleAdsRow, GoogleAdsServiceClient, SearchGoogleAdsStreamRequest}
import com.google.auth.oauth2.UserCredentials
import com.google.protobuf.util.JsonFormat
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import pureconfig.ConfigSource

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDate
import java.util.Base64
import scala.collection.JavaConverters._

case class GoogleClientParameters(
  developerToken: String,
  clientId: String,
  clientSecret: String,
  refreshToken: String,
  loginCustomerId: Long
)

case class Config(
  google: GoogleClientParameters
)
object Config {
  def build(): Config = {
    import pureconfig.generic.auto._
    // hidden development configuration with credentials
    ConfigSource
      .file(".env.application.conf")
      .loadOrThrow[Config]
  }
}

object Main {
  def initClient(credential: GoogleClientParameters): GoogleAdsServiceClient = {
    val clientCredentials = UserCredentials
      .newBuilder()
      .setClientId(credential.clientId)
      .setClientSecret(credential.clientSecret)
      .setRefreshToken(credential.refreshToken)
      .build()

    GoogleAdsClient
      .newBuilder()
      .setCredentials(clientCredentials)
      .setDeveloperToken(credential.developerToken)
      .setLoginCustomerId(credential.loginCustomerId)
      .build()
      .getLatestVersion
      .createGoogleAdsServiceClient()
  }

  def queryClient(client: GoogleAdsServiceClient)(account: String, query: String): Iterator[GoogleAdsRow] = {
    val streamRequest = SearchGoogleAdsStreamRequest
      .newBuilder()
      .setCustomerId(account.replace("-", ""))
      .setQuery(query)
      .build()

    val cacheFile = Paths.get("outputs", "cached_report.b64")

    val isCached = {
      if (Files.exists(cacheFile)) {
        val fileReader = Files.newBufferedReader(cacheFile)
        val base64CachedQuery = fileReader.readLine()
        val cachedQuery = new String(Base64.getDecoder.decode(base64CachedQuery), StandardCharsets.UTF_8)

        cachedQuery == query
      } else false
    }

    if (!isCached) {
      val encoder = Base64.getEncoder
      val printWriter = Files.newBufferedWriter(cacheFile)

      def writeLine(array: Array[Byte]): Unit = {
        val b64 = new String(encoder.encode(array), StandardCharsets.UTF_8)
        printWriter.append(b64)
        printWriter.append('\n')
      }

      writeLine(query.getBytes(StandardCharsets.UTF_8))
      try client
        .searchStreamCallable()
        .call(streamRequest)
        .iterator()
        .asScala
        .flatMap(_.getResultsList.asScala)
        .foreach(row => {
          val rowBytes = row.toByteArray
          writeLine(rowBytes)
          row
        })
      finally printWriter.close()
    } else {
      println("Query was cached - reusing previous result")
    }

    new Iterator[GoogleAdsRow] {
      private val reader = Files.newBufferedReader(cacheFile)
      private val iterator = reader.lines().iterator().asScala.drop(1)
      private val decoder = Base64.getDecoder

      private def decodeLine(line: String): GoogleAdsRow = GoogleAdsRow.parseFrom(decoder.decode(line))

      def hasNext: Boolean = {
        val hasNext = iterator.hasNext
        if (!hasNext) reader.close()
        hasNext
      }

      def next(): GoogleAdsRow = decodeLine(iterator.next())
    }
  }

  def structureMetadataSubtree(reportDate: LocalDate): Message =
    Message(
      "metadata",
      Map(
        "download_date" -> Constant(Schemas.String.nullable, reportDate.toString),
        "report_date" -> Constant(Schemas.String.nullable, reportDate.toString)
      )
    )

  def performanceMetadataSubtree(reportDate: LocalDate): Message =
    UserDefinedMappings.Message(
      "metadata",
      Map[String, UserDefinedMappings.UserDefinedProtoToAvroNode](
        "download_date" -> UserDefinedMappings.Constant(Schemas.String.nullable, reportDate.toString),
        "report_date" -> UserDefinedMappings.Scalar(GoogleAdsRow.getDescriptor, "segments.date")
      )
    )

  def main(args: Array[String]): Unit = {
    val outputPath = Paths.get("outputs")
    val googleJsonPath = outputPath.resolve("google-report.json")
    val avroToJsonPath = outputPath.resolve("avro-report.json")
    val avroPath = outputPath.resolve("report.avro")
    val config = Config.build()

    val fieldOrdering = new Ordering[String] {
      val idOrdering = List("campaign_id", "ad_group_id", "keyword_id", "ad_group_ad_ad_id")
      val lowestPriorityIdOrdering = idOrdering.size
      def priority(string: String): Int = {
        string match {
          case x if x.endsWith("_id") =>
            val idIndex = idOrdering.indexOf(x)
            if (idIndex == -1) lowestPriorityIdOrdering
            else idIndex
          case "metadata" => lowestPriorityIdOrdering + 2
          case _          => lowestPriorityIdOrdering + 1
        }
      }

      override def compare(x: String, y: String): Int = {
        val xPriority = priority(x)
        val yPriority = priority(y)
        if (xPriority == yPriority) Ordering.String.compare(x, y)
        else Ordering.Int.compare(xPriority, yPriority)
      }
    }

    val userDefinedMappings = List(
      // extract campaign id and ad_group_id
      "campaign_id" -> UserDefinedMappings.Scalar(GoogleAdsRow.getDescriptor, "customer.id"),
      "ad_group_id" -> UserDefinedMappings.Scalar(GoogleAdsRow.getDescriptor, "ad_group.id"),
      // add a struct called 'metadata'
      "metadata" -> UserDefinedMappings.Message(
        recordName = "metadata",
        Map(
          // add an author for the report
          "author" -> Constant(Schemas.String.nullable, "Ben"),
          // use a hardcoded date for the download date
          "download_date" -> UserDefinedMappings.Constant(Schemas.String.nullable, LocalDate.now().toString),
          // extract segments.date from the record
          "report_date" -> UserDefinedMappings.Scalar(GoogleAdsRow.getDescriptor, "segments.date")
        )
      )
    )

    val translator = ProtoToAvroTranslator(
      "com.getyourguide.google.ads",
      "example_campaign_report",
      GoogleAdsRow.getDescriptor,
      fields = CampaignCriterionStructureReport.fields,
      userDefinedMappings = userDefinedMappings,
      ordering = fieldOrdering
    )

    val client: (String, String) => Iterator[GoogleAdsRow] = queryClient(initClient(config.google))
    val rows = client("904-368-6249", CampaignCriterionStructureReport.queryString)

    println("Writing report")
    println(CampaignCriterionStructureReport.queryString)
    Files.createDirectories(outputPath)
    if (Files.exists(avroPath)) Files.delete(avroPath)

    val dataWriter = new GenericDatumWriter[GenericRecord](translator.schema)
    val fileWriter = new DataFileWriter[GenericRecord](dataWriter)
      .create(translator.schema, avroPath.toFile)

    val nRowsWritten = time("Time to build a report (avro)") {
      try rows
        .map(translator.translate)
        .map(fileWriter.append)
        .size
      finally fileWriter.close()
    }

    println(s"Wrote $nRowsWritten to $avroPath")

    {
      val printWriter = new PrintWriter(Files.newBufferedWriter(googleJsonPath))
      val jsonRows = client("904-368-6249", CampaignCriterionStructureReport.queryString)
      val printer = JsonFormat.printer().omittingInsignificantWhitespace()
      time("Time to build a report (json)") {
        try {
          jsonRows
            .map(printer.print)
            .foreach(printWriter.println)
        } finally {
          printWriter.close()
        }
      }
    }

    {
      val printWriter = new PrintWriter(Files.newBufferedWriter(avroToJsonPath))
      val jsonRows = client("904-368-6249", CampaignCriterionStructureReport.queryString)
      time("Time to build a report (avro-to-json)") {
        try {
          jsonRows
            .map(translator.translate)
            .foreach(printWriter.println)
        } finally {
          printWriter.close()
        }
      }
    }
  }
}
