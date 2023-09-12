package text2ql.api

import cats.implicits.catsSyntaxOptionId
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax._
import io.circe.{Codec, Decoder, Encoder}
import sttp.tapir.Schema

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.util.Try

case class AskResponsePayload(
    grid: Option[GridWithDataRenderTypeResponseModel] = None,
    pagination: Option[ChatMessageRequestModel] = None
)

sealed trait GridPropertyDataType

case class GridPropertyDataTypeString(dataType: String = "string")   extends GridPropertyDataType
case class GridPropertyDataTypeNumber(dataType: String = "number")   extends GridPropertyDataType
case class GridPropertyDataTypeLookup(dataType: String = "lookup")   extends GridPropertyDataType
case class GridPropertyDataTypeDate(dataType: String = "date")       extends GridPropertyDataType
case class GridPropertyDataTypeBoolean(dataType: String = "boolean") extends GridPropertyDataType

sealed trait GridPropertyFilterType

case class GridPropertyFilterTypeBoolean(filterType: String = "booleanSelect")         extends GridPropertyFilterType
case class GridPropertyFilterTypeLookupSelect(filterType: String = "lookupSelect")     extends GridPropertyFilterType
case class GridPropertyFilterTypeString(filterType: String = "string")                 extends GridPropertyFilterType
case class GridPropertyFilterTypeStringSelect(filterType: String = "stringSelect")     extends GridPropertyFilterType
case class GridPropertyFilterTypeDateInterval(filterType: String = "dateInterval")     extends GridPropertyFilterType
case class GridPropertyFilterTypeNumberInterval(filterType: String = "numberInterval") extends GridPropertyFilterType

sealed trait IDataRenderType

case class IDataRenderTypeTable(renderType: String = "table") extends IDataRenderType
case class IDataRenderTypeChart(renderType: String = "chart") extends IDataRenderType

sealed trait GridPropertyFilterValue

case class GridPropertyFilterValueString(value: String)           extends GridPropertyFilterValue
case class GridPropertyFilterValueNumber(value: Double)           extends GridPropertyFilterValue
case class GridPropertyFilterValueBoolean(value: Boolean)         extends GridPropertyFilterValue
case class GridPropertyFilterValueDate(value: LocalDateTime)      extends GridPropertyFilterValue
case class GridPropertyFilterValueLookupModel(value: LookupModel) extends GridPropertyFilterValue
case class GridPropertyFilterValueNull()                          extends GridPropertyFilterValue
case class LookupModel(id: String, title: String)

case class GridWithDataRenderTypeResponseModel(
    renderType: IDataRenderType = IDataRenderTypeTable(),
    properties: List[GridPropertyItemModel] = List.empty[GridPropertyItemModel],
    items: List[Map[String, GridPropertyFilterValue]] = List.empty[Map[String, GridPropertyFilterValue]],
    title: Option[String] = None,
    total: Long = 0L
)

case class GridPropertyItemModel(
    key: String,
    title: String,
    dataType: GridPropertyDataType,
    filter: Option[GridPropertyFilterModel] = None
)

case class GridPropertyFilterModel(
    `type`: GridPropertyFilterType,
    values: Option[List[GridPropertyFilterValue]] = None
)

case class AskRepoResponse(
    text: Option[String] = None,
    custom: Option[AskResponsePayload] = None,
    count: CountQueryDTO,
    query: Option[String] = None
)

object GridPropertyDataType {

  implicit val encodeGridPropertyDataType: Encoder[GridPropertyDataType] = Encoder.instance {
    case _ @GridPropertyDataTypeString(dataType)  => dataType.asJson
    case _ @GridPropertyDataTypeNumber(dataType)  => dataType.asJson
    case _ @GridPropertyDataTypeLookup(dataType)  => dataType.asJson
    case _ @GridPropertyDataTypeDate(dataType)    => dataType.asJson
    case _ @GridPropertyDataTypeBoolean(dataType) => dataType.asJson
  }

  implicit val decodeGridPropertyDataType: Decoder[GridPropertyDataType] =
    Decoder[String].map {
      case "number"  => GridPropertyDataTypeNumber()
      case "lookup"  => GridPropertyDataTypeLookup()
      case "date"    => GridPropertyDataTypeDate()
      case "boolean" => GridPropertyDataTypeBoolean()
      case _         => GridPropertyDataTypeString()
    }

  implicit val schema: Schema[GridPropertyDataType] = Schema.derived

  def fromType(attrType: String): GridPropertyDataType = attrType match {
    case "double" | "long" => GridPropertyDataTypeNumber()
    case "boolean"         => GridPropertyDataTypeBoolean()
    case "datetime"        => GridPropertyDataTypeDate()
    case _                 => GridPropertyDataTypeString()
  }
}

object GridPropertyFilterType {

  implicit val encodeGridPropertyFilterType: Encoder[GridPropertyFilterType] = Encoder.instance {
    case _ @GridPropertyFilterTypeBoolean(filterType)        => filterType.asJson
    case _ @GridPropertyFilterTypeLookupSelect(filterType)   => filterType.asJson
    case _ @GridPropertyFilterTypeStringSelect(filterType)   => filterType.asJson
    case _ @GridPropertyFilterTypeString(filterType)         => filterType.asJson
    case _ @GridPropertyFilterTypeDateInterval(filterType)   => filterType.asJson
    case _ @GridPropertyFilterTypeNumberInterval(filterType) => filterType.asJson
  }

  implicit val decodeGridPropertyFilterType: Decoder[GridPropertyFilterType] =
    Decoder[String].map {
      case "dateInterval"   => GridPropertyFilterTypeDateInterval()
      case "numberInterval" => GridPropertyFilterTypeNumberInterval()
      case "booleanSelect"  => GridPropertyFilterTypeBoolean()
      case "lookupSelect"   => GridPropertyFilterTypeLookupSelect()
      case "stringSelect"   => GridPropertyFilterTypeStringSelect()
      case _                => GridPropertyFilterTypeString()
    }

  implicit val schema: Schema[GridPropertyFilterType] = Schema.derived
}

object IDataRenderType {

  implicit val encodeGridPropertyRenderType: Encoder[IDataRenderType] = Encoder.instance {
    case _ @IDataRenderTypeTable(renderType) => renderType.asJson
    case _ @IDataRenderTypeChart(renderType) => renderType.asJson
  }

  implicit val decodeGridPropertyFilterType: Decoder[IDataRenderType] =
    Decoder[String].map {
      case "chart" => IDataRenderTypeChart()
      case _       => IDataRenderTypeTable()
    }

  implicit val schema: Schema[IDataRenderType] = Schema.derived
}

object GridPropertyFilterValue extends TimestampCodec {

  val nullValueDescription: String = "нет данных"

  implicit val encodeGridPropertyFilterValue: Encoder[GridPropertyFilterValue] = Encoder.instance {
    case _ @GridPropertyFilterValueNull()                    => nullValueDescription.asJson
    case _ @GridPropertyFilterValueString(renderType)        => renderType.asJson
    case _ @GridPropertyFilterValueNumber(renderType)        => renderType.asJson
    case _ @GridPropertyFilterValueBoolean(renderType)       => renderType.asJson
    case _ @GridPropertyFilterValueDate(renderType)          => Timestamp.valueOf(renderType).asJson
    case lookupModel @ GridPropertyFilterValueLookupModel(_) => lookupModel.asJson
  }

  implicit val decodeGridPropertyFilterValue: Decoder[GridPropertyFilterValue] =
    Decoder[String]
      .map[GridPropertyFilterValue](GridPropertyFilterValueString)
      .or(Decoder[Double].map[GridPropertyFilterValue](GridPropertyFilterValueNumber))
      .or(Decoder[Boolean].map[GridPropertyFilterValue](GridPropertyFilterValueBoolean))
      .or(Decoder[LocalDateTime].map[GridPropertyFilterValue](GridPropertyFilterValueDate))
      .or(Decoder.forProduct1("value")(GridPropertyFilterValueLookupModel.apply))

  implicit val schema: Schema[GridPropertyFilterValue] = Schema.derived

  def fromValueAndType(value: String, attrType: String): GridPropertyFilterValue = attrType match {
    case _ if value == null => GridPropertyFilterValueNull()
    case "double" | "long"  =>
      value.toDoubleOption
        .fold[GridPropertyFilterValue](GridPropertyFilterValueString(value))(GridPropertyFilterValueNumber)
    case "boolean"          =>
      val fixedValue = value match {
        case "t" => "True"
        case "f" => "False"
        case _   => value
      }
      fixedValue.toBooleanOption
        .fold[GridPropertyFilterValue](GridPropertyFilterValueString(fixedValue))(GridPropertyFilterValueBoolean)
    case "datetime"         =>
      Try(LocalDateTime.parse(value)).toOption
        .fold[GridPropertyFilterValue](GridPropertyFilterValueString(value))(d => GridPropertyFilterValueDate(d))
    case _                  => GridPropertyFilterValueString(value)
  }

  def getStringValue(fv: GridPropertyFilterValue): String = fv match {
    case GridPropertyFilterValueNull()             => nullValueDescription
    case GridPropertyFilterValueString(value)      => value
    case GridPropertyFilterValueNumber(value)      => value.toString
    case GridPropertyFilterValueBoolean(value)     => value.toString
    case GridPropertyFilterValueDate(value)        => value.toString
    case GridPropertyFilterValueLookupModel(value) => value.title
  }

}

object GridWithDataRenderTypeResponseModel {
  implicit val codec: Codec[GridWithDataRenderTypeResponseModel]   = deriveCodec
  implicit val schema: Schema[GridWithDataRenderTypeResponseModel] = Schema.derived
}

object GridPropertyItemModel {
  implicit val codec: Codec[GridPropertyItemModel]   = deriveCodec
  implicit val schema: Schema[GridPropertyItemModel] = Schema.derived
}

object GridPropertyFilterModel {
  implicit val codec: Codec[GridPropertyFilterModel]   = deriveCodec
  implicit val schema: Schema[GridPropertyFilterModel] = Schema.derived

  def fromAttrType(attrType: String, isCategorical: Boolean): Option[GridPropertyFilterModel] = attrType match {
    case "double" | "long"         => GridPropertyFilterModel(GridPropertyFilterTypeNumberInterval()).some
    case "boolean"                 => GridPropertyFilterModel(GridPropertyFilterTypeBoolean()).some
    case "string" if isCategorical => GridPropertyFilterModel(GridPropertyFilterTypeStringSelect()).some
    case "string"                  => GridPropertyFilterModel(GridPropertyFilterTypeString()).some
    case "datetime"                => GridPropertyFilterModel(GridPropertyFilterTypeDateInterval()).some
    case _                         => None
  }
}

object GridPropertyFilterValueLookupModel {
  implicit val codec: Codec[GridPropertyFilterValueLookupModel]   = deriveCodec
  implicit val schema: Schema[GridPropertyFilterValueLookupModel] = Schema.derived
}

object LookupModel {
  implicit val codec: Codec[LookupModel]   = deriveCodec
  implicit val schema: Schema[LookupModel] = Schema.derived
}

object AskResponsePayload {
  implicit val codec: Codec[AskResponsePayload]   = deriveCodec
  implicit val schema: Schema[AskResponsePayload] = Schema.derived
}
