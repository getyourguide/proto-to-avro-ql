package com.getyourguide.proto.to.avro

import com.google.ads.googleads.v7.common.Segments
import com.google.ads.googleads.v7.resources.{AdGroup, Campaign, Customer}
import com.google.ads.googleads.v7.services.GoogleAdsRow
import org.apache.avro.generic.GenericRecord
import org.scalatest.funspec.AnyFunSpec

import scala.collection.JavaConverters._

class ProtoToAvroMappingTest extends AnyFunSpec {
  def row(customerId: Long, campaignId: Long, campaignName: String, adGroupId: Int, date: String): GoogleAdsRow =
    GoogleAdsRow
      .newBuilder()
      .setCustomer(
        Customer
          .newBuilder()
          .setId(customerId)
          .setDescriptiveName("ignored_junk")
          .build()
      )
      .setCampaign(
        Campaign
          .newBuilder()
          .setId(campaignId)
          .setName(campaignName)
          .build()
      )
      .setAdGroup(
        AdGroup
          .newBuilder()
          .setId(adGroupId)
          .build()
      )
      .setSegments(
        Segments
          .newBuilder()
          .setDate(date)
          .build()
      )
      .build()

  describe("end-to-end mapping with fields") {
    import Schemas._
    it("can map protobuf to avro") {
      val testRow = row(customerId = 1, campaignId = 2, campaignName = "test-campaign", adGroupId = 3, "2021-01-01")
      val fields = List(
        "customer.id",
        "campaign.id",
        "campaign.name",
        "ad_group.id"
      )

      val expectedRunDate = "2021-01-01"
      val translator = ProtoToAvroTranslator(
        "test_namespace",
        "test_row",
        GoogleAdsRow.getDescriptor,
        fields,
        List(
          "test_run" -> UserDefinedMappings.Constant(Schemas.Int, 1.asInstanceOf[AnyRef]),
          "customer_id" -> UserDefinedMappings.Constant(Schemas.String.nullable, 1.asInstanceOf[AnyRef]),
          "campaign_id" -> UserDefinedMappings.Scalar(GoogleAdsRow.getDescriptor, "campaign.id"),
          "metadata" -> UserDefinedMappings.Message(
            "metadata",
            Map(
              "run_date" -> UserDefinedMappings.Constant(Schemas.String.nullable, expectedRunDate),
              "report_date" -> UserDefinedMappings.Scalar(GoogleAdsRow.getDescriptor, "segments.date")
            )
          )
        )
      )

      val result = translator.translate(testRow)

      println(result.toString)

      val customer = result.get("customer").asInstanceOf[GenericRecord]
      val campaign = result.get("campaign").asInstanceOf[GenericRecord]
      val adGroup = result.get("ad_group").asInstanceOf[GenericRecord]
      val metadata = result.get("metadata").asInstanceOf[GenericRecord]

      assert(result.get("test_run") === 1)
      // check extracted ids match existing ids
      assert(customer.get("id") === result.get("customer_id"))
      assert(campaign.get("id") === result.get("campaign_id"))

      assert(metadata.get("run_date") === expectedRunDate)
      assert(metadata.get("report_date") === testRow.getSegments.getDate)

      assert(customer.get("id") === testRow.getCustomer.getId)
      assert(campaign.get("name") === testRow.getCampaign.getName)
      assert(campaign.get("id") === testRow.getCampaign.getId)
      assert(adGroup.get("id") === testRow.getAdGroup.getId)

      def getFieldNames(record: GenericRecord): List[String] = record.getSchema.getFields.asScala.map(_.name()).toList
      // check only the expected fields were extracted
      assert(getFieldNames(customer) === List("id"))
      assert(getFieldNames(campaign) === List("id", "name"))
      assert(getFieldNames(adGroup) === List("id"))
    }
  }
}
