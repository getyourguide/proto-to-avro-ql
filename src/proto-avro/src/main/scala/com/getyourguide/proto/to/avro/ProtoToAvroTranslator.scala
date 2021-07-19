package com.getyourguide.proto.to.avro

import com.getyourguide.proto.to.avro.ProtoMessageToAvroMappings.{ProtoToAvroMessage, ProtoToAvroNode}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.GeneratedMessageV3
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.collection.JavaConverters._

case class ProtoToAvroTranslator(namespace: String, name: String, branches: List[(String, ProtoToAvroNode)]) {
  val schema: Schema = {
    val innerSchema = Schema.createRecord(name, null, namespace, false)
    val fields = branches.map { case (fieldName, branch) => branch.toField(fieldName) }
    innerSchema.setFields(fields.asJava)
    innerSchema
  }

  private val builder = new GenericRecordBuilder(schema)

  /**
    * Build an avro row from proto where each branch of the root corresponds to a field in that proto
    * @param proto the proto message to convert
    * @return a generic record with the schema of the root node and contents sourced from the proto
    */
  def translate(proto: GeneratedMessageV3): GenericRecord = {
    branches.foreach {
      case (branchName, protoNode) =>
        val branchValue = protoNode.getRowField(proto)
        builder.set(branchName, branchValue)
    }
    builder.build()
  }
}

object ProtoToAvroTranslator {

  /**
    * Given a set of fields in the format x.y.z, return a tree where each branch exists in the set of input
    * fields, or, if a message occurs at the end of a path, where the branch is all the fields of the Message at that
    * path.
    *
    * @param fields the fields to extract
    * @return a list of mappers and their names in the structure
    */
  def buildFieldMappers(descriptor: Descriptor, fields: List[String]): List[(String, ProtoToAvroNode)] = {
    val seen = Map.empty[FieldDescriptor, ProtoToAvroMessage]
    val (_, children) = ProtoToAvroNode.buildChildren(seen, fields, descriptor)
    children
  }

  /**
    * Build a translator
    * @param namespace the namespace for the record
    * @param name the name for the record
    * @param descriptor the descriptor to extract fields from
    * @param fields the fields to extract
    * @param userDefinedMappings an optional list of user defined mappings
    * @param ordering an optional ordering. If none is provided, lexicographical ordering will be used
    * @return a translator which can map from proto records to avro
    */
  def apply(
    namespace: String,
    name: String,
    descriptor: Descriptor,
    fields: List[String],
    userDefinedMappings: List[(String, UserDefinedMappings.UserDefinedProtoToAvroNode)] = List.empty,
    ordering: Ordering[String] = Ordering.String
  ): ProtoToAvroTranslator = {
    val messageFieldMappers = buildFieldMappers(descriptor, fields)
    val fieldMappers = (userDefinedMappings ++ messageFieldMappers).sortBy(_._1)(ordering)

    ProtoToAvroTranslator(namespace, name, fieldMappers)
  }
}
