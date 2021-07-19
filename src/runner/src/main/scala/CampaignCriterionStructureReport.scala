object CampaignCriterionStructureReport {
  val fields = List(
    "customer.currency_code",
    "customer.descriptive_name",
    "customer.id",
    "customer.time_zone",
    "campaign.id",
    "campaign.name",
    "campaign.status",
    "ad_group.id",
    "ad_group.name",
    "ad_group.status",
    "geographic_view.country_criterion_id",
    "geographic_view.location_type",
    "metrics.all_conversions",
    "metrics.all_conversions_from_interactions_rate",
    "metrics.all_conversions_value",
    "metrics.average_cost",
    "metrics.average_cpc",
    "metrics.average_cpm",
    "metrics.average_cpv",
    "metrics.clicks",
    "metrics.conversions",
    "metrics.conversions_from_interactions_rate",
    "metrics.conversions_value",
    "metrics.cost_micros",
    "metrics.cost_per_all_conversions",
    "metrics.cost_per_conversion",
    "metrics.cross_device_conversions",
    "metrics.ctr",
    "metrics.impressions",
    "metrics.interaction_event_types",
    "metrics.interaction_rate",
    "metrics.interactions",
    "metrics.value_per_all_conversions",
    "metrics.value_per_conversion",
    "metrics.video_view_rate",
    "metrics.video_views",
    "metrics.view_through_conversions",
    "segments.ad_network_type",
    "segments.date",
    "segments.device",
    "segments.geo_target_city",
    "segments.geo_target_most_specific_location",
    "segments.geo_target_region"
  )

  val queryString =
    s"SELECT ${fields.mkString(",")} FROM geographic_view " +
      s"WHERE segments.date BETWEEN '2021-02-01' AND '2021-03-01' " +
      s"AND metrics.impressions > 0"
}
