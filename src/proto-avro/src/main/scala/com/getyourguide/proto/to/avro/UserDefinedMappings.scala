package com.getyourguide.proto.to.avro

import com.getyourguide.proto.to.avro.ProtoMessageToAvroMappings.{ProtoToAvroLeaf, ProtoToAvroNode}
import com.getyourguide.proto.to.avro.Schemas.SchemaOps
import com.google.protobuf.Descriptors.FieldDescriptor.Type.MESSAGE
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.GeneratedMessageV3
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.util.internal.Accessor

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * UserDefinedMapping nodes allow redefining the message structure, creating a mapping from a proto message to an avro
  * message. For instance, you might add custom nodes to the graph to define metadata such as the date the query was run,
  * to add trace fields to rows, or to flatten the structure.
  *
  * This is an experimental API, and is liable to change.
  *
  * Currently supports:
  * - definition of custom messages composed from mapping nodes
  * - extraction of scalar fields
  * - adding constant fields
  */
object UserDefinedMappings {

  /**
    * A family of nodes where each node expects the passed value to map to be the root instance of a message.
    * Allows custom mappings at the cost of complexity in the scalar values
    */
  trait UserDefinedProtoToAvroNode extends ProtoToAvroNode

  /**
    * A node which evaluates to a constant value. The user is required to specify the schema
    * @param schema the schema of the value
    * @param value the constant value
    */
  case class Constant(schema: Schema, value: AnyRef) extends UserDefinedProtoToAvroNode {
    override def toField(fieldName: String): Schema.Field = Accessor.createField(fieldName, schema, null, getDefault)

    override def getRowField(parentProto: GeneratedMessageV3): AnyRef = value
  }

  object Scalar {
    def apply(descriptor: Descriptor, path: String): Scalar = {
      val elements = path.split('.')
      val fieldPaths = ListBuffer[FieldDescriptor]()
      elements.foreach(element => {
        val fieldDescriptor =
          fieldPaths.lastOption match {
            case Some(value) => value.getMessageType.findFieldByName(element)
            case None        => descriptor.findFieldByName(element)
          }
        fieldPaths.append(fieldDescriptor)
      })

      Scalar(descriptor, fieldPaths.toList)
    }
  }

  case class Scalar(descriptor: Descriptor, fieldPath: List[FieldDescriptor]) extends UserDefinedProtoToAvroNode {
    def validateFieldPath(): Unit = {
      if (fieldPath.isEmpty) throw new IllegalArgumentException("Expected a non empty list of fields")
      val pathIsConsistent = fieldPath.reverse.sliding(2).forall {
        case List(first, second) => first.getContainingType == second.getMessageType
      }
      if (!pathIsConsistent) throw new IllegalArgumentException("The path must be continuous path from child to parent")
      if (!fieldPath.headOption.exists(_.getContainingType == descriptor))
        throw new IllegalArgumentException("The first element's parent must be the expected descriptor")

      if (fieldPath.lastOption.exists(_.getType == MESSAGE))
        throw new IllegalArgumentException("The last element's type must be a scalar value")

      if (fieldPath.exists(_.isRepeated))
        throw new IllegalArgumentException("None of the field elements may be repeated")
    }
    validateFieldPath()
    private val node = ProtoToAvroLeaf(fieldPath.last)

    override def schema: Schema = node.schema

    override def toField(fieldName: String): Schema.Field = node.toField(fieldName)

    /**
      * Take the parent proto, and use it to extract the node value. For instance, a GenericRecord (for a message),
      * or a primitive value
      *
      * @param parentProto the parent proto message
      * @return the extracted value
      */
    override def getRowField(parentProto: GeneratedMessageV3): AnyRef = {
      parentProto match {
        case row if row.getDescriptorForType == descriptor =>
          fieldPath.foldLeft[AnyRef](row) {
            case (elem, field) =>
              elem match {
                case e: GeneratedMessageV3 => e.getField(field)
                case other                 => other
              }
          }
        case _ =>
          throw new IllegalArgumentException("Expected to get the root descriptor for a generic scalar")
      }
    }
  }

  /**
    * A subtree allowing extension of the proto generation to add custom struct types. For instance, a user
    * could use this to add constants, or pull out values within the protobuf. A GenericProtoMessage expects
    * to receive the root descriptor as its parentProto, and the mapping functions should map message instances
    * to the parents of their respective nodes
    *
    * Example:
    * GenericProtoMessage(
    *   recordName = "metadata",
    *   mappings = Map(
    *      // a run ID inferred before tree construction
    *     "runId" -> ConstantNode.mapper(com.getyourguide.proto.to.avro.Schemas.Int, runId),
    *     // a date which is always set under .segments.date
    *     "reportDate" -> (_.getSegments, ProtoLeaf(Segments.getDescriptor.findFieldByName("date")))
    *   )
    *
    * @param mappings a map of fieldName -> (mapper, protoNode) instances. The mapper should map from the root protobuf
    *                 to the parent protobuf of the protoNode
    */
  case class Message(
    recordName: String,
    mappings: Map[String, UserDefinedProtoToAvroNode]
  ) extends UserDefinedProtoToAvroNode {
    val recordSchema: Schema = {
      val schema = Schema.createRecord(recordName, null, "com.getyourguide.generic", false)
      val fields = mappings.toList
        .map { case (fieldName, node) => node.toField(fieldName) }
      schema.setFields(fields.asJava)
      schema
    }

    override val schema: Schema = recordSchema.nullable

    private lazy val recordBuilder = new GenericRecordBuilder(recordSchema)
    override def getRowField(message: GeneratedMessageV3): AnyRef = {
      mappings
        .map {
          case (fieldName, branch) =>
            val branchValue = branch.getRowField(message)
            recordBuilder.set(fieldName, branchValue)
        }
      recordBuilder.build()
    }

    override def toField(fieldName: String): Schema.Field = Accessor.createField(fieldName, schema, null, getDefault)
  }
}
