package text2ql.api

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

import java.util.UUID
import scala.reflect.ClassTag

case class DataForDBQuery(
    requestId: UUID,
    entityList: List[EntityForDBQuery],
    relationList: List[RelationForDBQuery],
    pagination: Option[ChatMessageRequestModel] = None,
    logic: AggregationLogic,
    count: Option[CountQueryDTO] = None,
    domain: Domain,
    requestType: UserRequestType
)

case class EntityForDBQuery(
    entityName: String,
    attributes: List[AttributeForDBQuery] = List.empty[AttributeForDBQuery],
    isParent: Boolean = false,
    isTargetEntity: Boolean = false,
    includeGetClause: Boolean
)

case class RelationForDBQuery(
    relationName: String,
    attributes: List[AttributeForDBQuery] = List.empty[AttributeForDBQuery],
    entities: List[String],
    isTargetRelation: Boolean = false,
    includeGetClause: Boolean
)

case class AttributeForDBQuery(
    attributeName: String,
    includeGetClause: Boolean,
    attributeValues: Option[List[AttributeValue]] = None,
    fullTextItems: List[FullTextItem] = List.empty[FullTextItem],
    isTargetAttribute: Boolean = false,
    isAttributeFromRequest: Boolean = false,
    attributePriority: AttributePriority = DefaultPriority(),
    datePart: Option[String] = None
)

case class AttributeForDBQueryWithThingName(
    attribute: AttributeForDBQuery,
    thingName: String
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

sealed trait AttributePriority extends Product with Serializable

case class GeneralizingEntityHeadlinePriority(priorityLevel: Int = 1, sort: Int) extends AttributePriority
case class TargetEntityHeadlinePriority(priorityLevel: Int = 2, sort: Int)       extends AttributePriority
case class TargetAttributePriority(priorityLevel: Int = 3)                       extends AttributePriority
case class HeadlinePriority(priorityLevel: Int = 4, sort: Int)                   extends AttributePriority
case class FromRequestPriority(priorityLevel: Int = 5)                           extends AttributePriority
case class VisualPriority(priorityLevel: Int = 6, sort: Int)                     extends AttributePriority
case class DefaultPriority(priorityLevel: Int = 7)                               extends AttributePriority

case class AggregationLogic(
    unique: Boolean,
    targetAttr: String,
    targetThing: String,
    groupByAttr: String,
    groupByThing: String,
    subAttrOpt: Option[String] = None,
    aggregationFilterOpt: Option[AggregationFilter],
    sortModelOpt: Option[BaseSortModel],
    visualization: TagsVisualization = TagsVisualization(),
    distinct: Boolean = false,
    requestType: UserRequestType = UserRequestType.GetInstanceList
)

case class AggregationFilter(
    comparisonOperator: String,
    value: String
)

case class TagsVisualization(
    tags: List[String] = List.empty[String],
    nPrimaryTags: Int = 5
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

object AttributePriority {
  implicit val codec: Codec[AttributePriority]   = deriveCodec
  implicit val schema: Schema[AttributePriority] = Schema.derived

  def filterPriority[T <: AttributePriority: ClassTag](t: AttributePriority): Boolean = t match {
    case _: T => true
    case _    => false
  }
}

object AggregationLogic {
  implicit val codec: Codec[AggregationLogic]   = deriveCodec
  implicit val schema: Schema[AggregationLogic] = Schema.derived
}

object AggregationFilter {
  implicit val codec: Codec[AggregationFilter]   = deriveCodec
  implicit val schema: Schema[AggregationFilter] = Schema.derived
}

object TagsVisualization {
  implicit val codec: Codec[TagsVisualization]   = deriveCodec
  implicit val schema: Schema[TagsVisualization] = Schema.derived
}
