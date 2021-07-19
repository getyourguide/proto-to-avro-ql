# proto-to-avro-ql

## Overview

This library provides methods to transform Protobuf messages into Avro messages given a list of fields. It also supports
converting to JSON through the `.toString` method of a `GenericRecord`.

To convert a message to avro, you need a Descriptor for the message, a list of fields to convert, and a namespace and
name for the output record. A field is a sequence of '.' separated words forming a path into the proto.

For instance given the following protobuf:

```proto
message Row {
  optional Campaign campaign = 1;
  optional AdGroup ad_group = 2;
  optional Segments segments = 3;
}

message Campaign {
  int32 id = 1;
  string name = 2;
}

message AdGroup {
  int32 campaign_id = 1;
  int32 id = 2;
  string name = 3;
}

message Segments {
  string date = 1
}
```

One field might be `campaign.id`. A list of fields might be `List("campaign.id", "ad_group.id", "ad_group.name")`. The
output Avro record would be:

```json
{
  "campaign": {
    "id": "Campaign.id"
  },
  "ad_group": {
    "id": "AdGroup.id",
    "name": "AdGroup.name"
  }
}
```

### Motivation

The goal of this library is to provide a way to define a consistent schema when generating Avro, and to do so in a way
which is both strongly typed and easy to define. Our specific use case was querying data from the GoogleAds API, which
uses the same query format to query different fields.

This method is strongly typed in that queries are checked against the passed descriptor at runtime, and easy to write in
that the fields map one-to-one with the input protobuf.

### Future Work

In the future I would like to explore ahead of time compilation, more advanced remapping functionality, and deep
dive the library performance.

# Quickstart

The following code samples are intended to provide a quick way to get started. In general you use the library in two steps.

- 1. Define a translation tree using a Descriptor and a list of fields to extract
- 2. Apply the tree to a series of rows to convert the rows from Protobuf to Avro

Given you're working with Avro, likely you then do some further transformations such as writing the data to a file.
    
```scala
// 1. define the translator
val fields = List(
  "campaign.id",
  "campaign.name",
  "ad_group.id",
  "ad_group.name",
  "segments.date"
)

val translator = ProtoToAvroTranslator(
  namespace = "com.example",
  name = "CampaignReport",
  GoogleAdsRow.getDescriptor,
  fields
)

// 2. use the translator
val rows = ??? // some Protobuf rows matching 'GoogleAdsRow'
val avroRows = rows.map(translator.buildRow) // returns GenericRecord instances

// 3. (sample) write the rows to a file
val dataWriter = new GenericDatumWriter[GenericRecord](translator.schema)
val fileWriter = new DataFileWriter[GenericRecord](dataWriter)
  .create(translator.schema, new File("outputs/report.avro"))

try {
  avroRows.foreach(fileWriter.append)
} finally {
  fileWriter.close()
}
```

## Advanced Example - UserDefinedMapping

In addition to direct extraction of fields from messages, you can also customise messages using UserDefinedMappings. These
support extracting scalar fields, message subfields, and adding constant fields.

For instance, in the previous example we might want to lift some ids to the top level to facilitate easier joins, and
we might want to add extra metadata to the record, for instance the team which owns the report, and the time at which it
was executed.

To facilitate this, the `ProtoToAvroTranslator` can take an optional map of `UserDefinedMappings`, and an optional ordering
specifying the order for top level fields in the output record.

Let's define some user defined mappings:

```scala
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
      "download_date" -> UserDefinedMappings.Constant(Schemas.String.nullable, currentDate.toString),
      // extract segments.date from the record
      "report_date" -> UserDefinedMappings.Scalar(GoogleAdsRow.getDescriptor, "segments.date")
    )
  )
)
```
Considering just the above data, without considering the message, we would expect the following Avro:
```json
{
  "campaign_id": "Campaign.id",
  "ad_group_id": "AdGroup.id",
  "metadata": {
    "author": "ben",
    "download_date": "the current date",
    "report_date": "Segments.date"
  }
}
```

When combining this with some field queries, we would expect the union of the previous Avro record with the above.

We can also provide an ordering function to determine how we want the rows to be ordered. Note that the below example shows
a 'real-world' usage, and is a little convoluted as a result. In practice any ordering can be specified, including explicitly
passing a list of top level nodes.

```scala
// order strings such that any field with _id comes first, metadata comes last, and everything else is in between
// if both fields satisfy that condition, use lexicographical ordering. If the id matches a specific ordering, use that,
// otherwise use a default fallback
val fieldOrdering = new Ordering[String] {
  val idOrdering = List("campaign_id", "ad_group_id", "keyword_id", "ad_group_ad_ad_id")
  val lowestPriorityIdOrdering = idOrdering.size
  def priority(string: String): Int = { string match {
      case x if x.endsWith("_id") => 
        val idIndex = idOrdering.indexOf(x)
        if (idIndex == -1) lowestPriorityIdOrdering
        else idIndex
      case "metadata"             => lowestPriorityIdOrdering + 2
      case _                      => lowestPriorityIdOrdering + 1
    }
  }

  override def compare(x: String, y: String): Int = {
    val xPriority = priority(x)
    val yPriority = priority(y)
    if (xPriority == yPriority) Ordering.String.compare(x, y)
    else Ordering.Int.compare(xPriority, yPriority)
  }
}

val fields = List(
  "campaign.id",
  "campaign.name",
  "ad_group.id",
  "ad_group.name",
  "segments.date"
)

val translator = ProtoToAvroTranslator(
  namespace = "com.example",
  name = "CampaignReport",
  GoogleAdsRow.getDescriptor,
  fields,
  // defined above
  userDefinedMappings,
  fieldOrdering
)
```

### Behaviour Notes

#### Fields

When converting a field, the type of the output will be inferred from the proto descriptor.

It's not currently possible to index into arrays. Fields can only be defined up to arrays, after which all elements of the array
will be kept as is (you can't filter their fields).

### Performance

We didn't run any proper benchmarks (yet). Directly converting 207971 rows from one of our reports took around 3.5s. While
this is anecdotal, it's fast enough for our use case.

Doing a similar conversion with `JsonFormat.printer()` took ~13s, and going to Json through Avro took ~6s. Note that this
kind of conversion is not equivalent, and that Google's JsonPrinter has significantly different behaviour in terms of
handling missing fields. For instance, for a missing Integer field, proto-to-avro-ql will return 0, while Google will
omit the field. This tends to make the Avro -> Json results larger (but if you're storing things, store them as Avro/Proto anyway!).

As with any performance metrics, the best thing to do is to run your own checks and see if it works for your use case.