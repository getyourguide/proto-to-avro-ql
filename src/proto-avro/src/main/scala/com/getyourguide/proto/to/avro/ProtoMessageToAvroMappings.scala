package com.getyourguide.proto.to.avro

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.getyourguide.proto.to.avro.Schemas.SchemaOps
import com.google.protobuf.Descriptors.FieldDescriptor.Type._
import com.google.protobuf.Descriptors.{Descriptor, EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.GeneratedMessageV3
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.avro.protobuf.ProtobufData
import org.apache.avro.util.internal.Accessor

import scala.collection.JavaConverters._

object Schemas {
  val Null: Schema = Schema.create(Schema.Type.NULL)
  val Bool: Schema = Schema.create(Schema.Type.BOOLEAN)
  val Float: Schema = Schema.create(Schema.Type.FLOAT)
  val Double: Schema = Schema.create(Schema.Type.DOUBLE)
  val String: Schema = Schema.create(Schema.Type.STRING)
  val Bytes: Schema = Schema.create(Schema.Type.BYTES)
  val Int: Schema = Schema.create(Schema.Type.INT)
  val Long: Schema = Schema.create(Schema.Type.LONG)

  implicit class SchemaOps(schema: Schema) {
    def nullable: Schema = Schema.createUnion(
      Null,
      schema
    )
  }
}

/**
  * Provides support for mapping proto rows to a Avro messages, stripping out unset rows and providing a consistent schema
  * for the entire message format. Additionally using [[UserDefinedMappings]] allows adding extra nodes and graph transformations
  * such as adding date metadata.
  */
object ProtoMessageToAvroMappings {
  private val Nodes = JsonNodeFactory.instance

  def nullableSchema(schema: Schema): Schema = Schema.createUnion(
    Schema.create(Schema.Type.NULL),
    schema
  )

  /**
    * Delegate namespace creation to avro's Protobuf libraries
    */
  def namespace(fieldDescriptor: FieldDescriptor): String = {
    ProtobufData.get().getNamespace(fieldDescriptor.getFile, fieldDescriptor.getContainingType)
  }

  /**
    * A ProtoNode forms part of a tree used to traverse a protobuf message and convert it to an avro message.
    */
  trait ProtoToAvroNode {
    def schema: Schema
    def toField(fieldName: String): Schema.Field

    /**
      * Take the parent proto, and use it to extract the node value. For instance, a GenericRecord (for a message),
      * or a primitive value
      * @param parentProto the parent proto message
      * @return the extracted value
      */
    def getRowField(parentProto: GeneratedMessageV3): AnyRef

    def getDefault: JsonNode = {
      // the first type must be the default in a union
      val firstSchema = if (schema.getType == Schema.Type.UNION) schema.getTypes.get(0) else schema
      firstSchema.getType match {
        case Schema.Type.NULL | Schema.Type.RECORD                                       => Nodes.nullNode()
        case Schema.Type.ARRAY                                                           => Nodes.arrayNode()
        case Schema.Type.INT | Schema.Type.LONG | Schema.Type.FLOAT | Schema.Type.DOUBLE => Nodes.numberNode(0)
        case Schema.Type.ENUM                                                            => Nodes.textNode(firstSchema.getEnumDefault)
        case Schema.Type.STRING | Schema.Type.BYTES                                      => Nodes.nullNode()
        case Schema.Type.BOOLEAN                                                         => Nodes.booleanNode(false)
        case _ =>
          throw new IllegalArgumentException(s"${firstSchema.getType} messages are not currently supported")
      }
    }
  }

  object ProtoToAvroNode {
    def splitFields(fields: List[String]): List[(String, List[String])] =
      fields
        .map(_.split('.').toList)
        .groupBy(_.head)
        .mapValues(_.map(_.tail.mkString(".")).filterNot(_.isEmpty))
        .toList

    def fieldIsEmptyMessage(descriptor: Descriptor, fieldName: String): Boolean = {
      val fieldDescriptor = descriptor.findFieldByName(fieldName)
      fieldDescriptor.getType == MESSAGE && fieldDescriptor.getMessageType.getFields.size() == 0
    }

    /**
      * Builds nodes for each child field of the [[descriptor]] in [[includedPaths]]. If [[includedPaths]] is empty, all
      * fields will be included. [[seen]] is a cache of all previously seen descriptors
      *
      * IncludedPaths should be a list of strings which are paths, where a path is a series of elements separated by '.'
      * Each head element will be extracted from the current descriptor, and the process is then recursively applied
      * to the child element.
      *
      * If the final element of a path is an empty message, we treat it as an error since including empty structs is
      * meaningless. Non explicitly included empty messages are skipped.
      *
      * @param previouslySeenMessages a cache of all previously seen descriptors, to allow supporting recursive schemas
      * @param includedPaths a list of child paths to be included. If empty, all fields will be included
      * @param descriptor the descriptor to build children for
      * @return an updated map of seen nodes, and a list mapping field names to nodes
      */
    def buildChildren(
      previouslySeenMessages: Map[FieldDescriptor, ProtoToAvroMessage],
      includedPaths: List[String],
      descriptor: Descriptor
    ): (Map[FieldDescriptor, ProtoToAvroMessage], List[(String, ProtoToAvroNode)]) = {
      val fields =
        if (includedPaths.nonEmpty) {
          val destructuredFields = splitFields(includedPaths)
          // check the user isn't asking for a field which is an empty struct, since we can't process those
          destructuredFields.foreach {
            case (fieldName, _) =>
              if (descriptor.findFieldByName(fieldName) == null) {
                throw new IllegalArgumentException(s"${descriptor.getFullName} has no field: '$fieldName'")
              }
              if (fieldIsEmptyMessage(descriptor, fieldName))
                throw new IllegalArgumentException(
                  s"${descriptor.getFullName}.$fieldName is explicitly asked for, but is an empty message"
                )
          }
          destructuredFields
        } else descriptor.getFields.asScala.map(field => (field.getName, List.empty[String]))

      fields
        .filterNot { case (fieldName, _) => fieldIsEmptyMessage(descriptor, fieldName) }
        .foldLeft((previouslySeenMessages, List.empty[(String, ProtoToAvroNode)])) {
          case ((aggSeen, aggNodes), (subFieldName, subFieldSubFields)) =>
            val subFieldDescriptor = descriptor.findFieldByName(subFieldName)
            val (newNode, newSeen) = ProtoToAvroNode(subFieldDescriptor, aggSeen, subFieldSubFields)
            (newSeen, aggNodes :+ (subFieldName, newNode))
        }
    }

    /**
      * From a fieldDescriptor, given a cache of already existing nodes, create a proto node for the
      * descriptor and it's children. includedFields is a list of fields to include in the newly
      * created node. If includedFields is empty, all fields will be included.
      * @param fieldDescriptor the field descriptor to parse
      * @param previouslySeenMessages
      * @param includedFields
      * @return
      */
    def apply(
      fieldDescriptor: FieldDescriptor,
      previouslySeenMessages: Map[FieldDescriptor, ProtoToAvroMessage],
      includedFields: List[String]
    ): (ProtoToAvroNode, Map[FieldDescriptor, ProtoToAvroMessage]) = {
      if (fieldDescriptor.getType == MESSAGE) {
        val descriptor = fieldDescriptor.getMessageType

        if (previouslySeenMessages.contains(fieldDescriptor)) {
          // if the node already exists in the tree, return the node to avoid recursing forever
          (previouslySeenMessages(fieldDescriptor), previouslySeenMessages)
        } else {
          // build a new message node, add it to the cache, then build its children
          val protoMessage = ProtoToAvroMessage(fieldDescriptor)
          val updatedSeen = previouslySeenMessages.updated(fieldDescriptor, protoMessage)
          val (newSeen, namedChildren) = buildChildren(updatedSeen, includedFields, descriptor)

          // this mutation is required so that the message can be referenced by the children before it's built
          protoMessage.setBranches(namedChildren)

          (protoMessage, newSeen)
        }
      } else {
        (ProtoToAvroLeaf(fieldDescriptor), previouslySeenMessages)
      }
    }
  }

  /**
    * Models a simple type, or a list of simple types. For instance, a list of ints, or a single int.
    * Doesn't allow MESSAGE types, and we don't consider groups (since they've been deprecated).
    *
    * @param fieldDescriptor the descriptor for the field
    */
  case class ProtoToAvroLeaf(fieldDescriptor: FieldDescriptor, schema: Schema) extends ProtoToAvroNode {
    private def buildEnum(proto: EnumValueDescriptor): EnumSymbol = new EnumSymbol(schema, proto.getName)

    /** Add this field to avro row currently being built
      * @param parentProto the parent message _containing_ the field
      */
    def getRowField(parentProto: GeneratedMessageV3): AnyRef = {
      val repeats = fieldDescriptor.isRepeated
      val nodeValue = parentProto.getField(fieldDescriptor)
      if (nodeValue == null) {
        getDefault
      } else {
        fieldDescriptor.getType match {
          case ENUM if repeats =>
            nodeValue
              .asInstanceOf[java.util.List[EnumValueDescriptor]]
              .asScala
              .map(buildEnum)
              .asJava
          case ENUM                                 => buildEnum(nodeValue.asInstanceOf[EnumValueDescriptor])
          case STRING if nodeValue.toString.isEmpty => null
          case _                                    => nodeValue
        }
      }
    }

    override def toField(fieldName: String): Schema.Field =
      Accessor.createField(fieldName, schema, null, getDefault)
  }

  object ProtoToAvroLeaf {
    def getSchema(fieldDescriptor: FieldDescriptor): Schema = {
      val schema = fieldDescriptor.getType match {
        case BOOL                                         => Schemas.Bool
        case FLOAT                                        => Schemas.Float
        case DOUBLE                                       => Schemas.Double
        case STRING                                       => Schemas.String
        case BYTES                                        => Schemas.Bytes
        case INT32 | UINT32 | SINT32 | FIXED32 | SFIXED32 => Schemas.Int
        case INT64 | UINT64 | SINT64 | FIXED64 | SFIXED64 => Schemas.Long
        case ENUM =>
          val enumType = fieldDescriptor.getEnumType
          val enumSymbols = enumType.getValues.asScala.map(_.toString).asJava
          Schema.createEnum(enumType.getName, null, namespace(fieldDescriptor), enumSymbols)
        case _ =>
          throw new IllegalStateException(
            s"Unexpected fieldtype: ${fieldDescriptor.getType} at ${fieldDescriptor.getFullName}"
          )
      }

      if (fieldDescriptor.isRepeated) Schema.createArray(schema)
      else if (!fieldDescriptor.isRequired && fieldDescriptor.getType == STRING) schema.nullable
      else schema
    }

    def apply(fieldDescriptor: FieldDescriptor): ProtoToAvroLeaf = {
      if (fieldDescriptor.getType == MESSAGE) {
        throw new IllegalStateException("A proto leaf cannot have a field that is a message type")
      }

      ProtoToAvroLeaf(fieldDescriptor, ProtoToAvroLeaf.getSchema(fieldDescriptor))
    }
  }

  /**
    * Models complex types, i.e structs (messages) or lists of structs
    * @param fieldDescriptor the field descriptor for this type
    */
  case class ProtoToAvroMessage(fieldDescriptor: FieldDescriptor) extends ProtoToAvroNode {
    require(fieldDescriptor.getType == MESSAGE)

    private val recordSchema: Schema = {
      Schema.createRecord(fieldDescriptor.getName, null, namespace(fieldDescriptor), false)
    }

    val schema: Schema = {
      if (fieldDescriptor.isRepeated) Schema.createArray(recordSchema)
      else if (fieldDescriptor.isOptional) recordSchema.nullable
      else recordSchema
    }

    private var innerBranches: Option[List[(String, ProtoToAvroNode)]] = None
    def branches: List[(String, ProtoToAvroNode)] = {
      innerBranches match {
        case Some(value) => value
        case None        => throw new IllegalStateException("Attempted to access unset field branches")
      }
    }

    def setBranches(branches: List[(String, ProtoToAvroNode)]): Unit = {
      innerBranches = Some(branches)
      val fields = branches
        .map { case (fieldName, branch) => branch.toField(fieldName) }
        .sortBy(_.name())

      recordSchema.setFields(fields.asJava)
    }

    // Optimisation - we reuse the record builder on each record to reduce the amount of objects created.
    // since the schema is not set on ProtoMessage creation, but after the children are created, the builder is lazy.
    private lazy val recordBuilder = new GenericRecordBuilder(recordSchema)

    /**
      * Build an Avro record from the input proto type
      * @param proto a proto message whose descriptor matches this node
      * @return a generic record for the node
      */
    def buildRecord(proto: GeneratedMessageV3): GenericRecord = {
      branches.foreach {
        case (fieldName, childNode) => recordBuilder.set(fieldName, childNode.getRowField(proto))
      }
      recordBuilder.build()
    }

    /**
      * Add this field to the Avro record currently being built using the input proto. Will add either a record, a list
      * of records, or null if the proto isn't set
      *
      * @param parentProto the parent proto of this row
      */
    def getRowField(parentProto: GeneratedMessageV3): AnyRef = {
      if (fieldDescriptor.isRepeated) {
        val nodeProto = parentProto.getField(fieldDescriptor).asInstanceOf[java.util.List[GeneratedMessageV3]]
        nodeProto.asScala.map(buildRecord).asJava
      } else if (parentProto.hasField(fieldDescriptor)) {
        val nodeProto = parentProto.getField(fieldDescriptor).asInstanceOf[GeneratedMessageV3]
        buildRecord(nodeProto)
      } else {
        null
      }
    }

    override def toField(fieldName: String): Schema.Field = Accessor.createField(fieldName, schema, null, getDefault)
  }

  /**
  * The root for the proto, which is the topmost descriptor considered. We need a separate type because the top level
  * class has no boxing field descriptor
  * @param namespace the namespace for the output record
  * @param name the name for the output record
  * @param branches a list of fields for the schema being considered
  */
}
