package text2ql.api

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema
import text2ql.domainschema.{DomainSchemaAttribute, DomainSchemaVertex}

import java.util.UUID

case class DataForDBQuery(
    requestId: UUID,
    domain: Domain,
    entityList: List[EntityForDBQuery],
    relationList: List[RelationForDBQuery],
    pagination: Option[ChatMessageRequestModel] = None,
    logic: AggregationLogic
)

case class EntityForDBQuery(
    entityName: String,
    attributes: List[AttributeForDBQuery] = List.empty[AttributeForDBQuery],
    isTargetEntity: Boolean = false
)

case class RelationForDBQuery(
    relationName: String,
    attributes: List[AttributeForDBQuery] = List.empty[AttributeForDBQuery],
    entities: List[String]
)

case class AttributeForDBQuery(
    attributeName: String,
    attributeValues: Option[List[AttributeValue]] = None,
    isTargetAttribute: Boolean = false
)

case class AttributeValue(
    value: String,
    comparisonOperator: String,
    joinValuesWithOr: Boolean = true
)

case class ThingWithOriginalName(
    originalName: String,
    thingName: String
)

case class ExtractedDataForAggregation(
    aggregationValue: String,
    headlineValue: String
)

case class AggregationLogic(
    unique: Boolean,
    targetAttr: String,
    targetThing: String,
    groupByAttr: String,
    groupByThing: String,
    sortModelOpt: Option[BaseSortModel],
    visualization: TagsVisualization = TagsVisualization()
)

case class TagsVisualization(
    tags: List[String] = List.empty[String]
)

object DataForDBQuery {
  implicit val codec: Codec[DataForDBQuery]   = deriveCodec
  implicit val schema: Schema[DataForDBQuery] = Schema.derived
}

object EntityForDBQuery {
  implicit val codec: Codec[EntityForDBQuery]   = deriveCodec
  implicit val schema: Schema[EntityForDBQuery] = Schema.derived
}

object RelationForDBQuery {
  implicit val codec: Codec[RelationForDBQuery]   = deriveCodec
  implicit val schema: Schema[RelationForDBQuery] = Schema.derived
}

object AttributeForDBQuery {
  implicit val codec: Codec[AttributeForDBQuery]   = deriveCodec
  implicit val schema: Schema[AttributeForDBQuery] = Schema.derived
}

object AttributeValue {
  implicit val codec: Codec[AttributeValue]   = deriveCodec
  implicit val schema: Schema[AttributeValue] = Schema.derived
}

object AggregationLogic {
  implicit val codec: Codec[AggregationLogic]   = deriveCodec
  implicit val schema: Schema[AggregationLogic] = Schema.derived
}

object TagsVisualization {
  implicit val codec: Codec[TagsVisualization]   = deriveCodec
  implicit val schema: Schema[TagsVisualization] = Schema.derived
}
