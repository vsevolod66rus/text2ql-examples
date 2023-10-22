package text2ql.api

import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax._
import io.circe.{Codec, Decoder, Encoder}
import sttp.tapir.Schema

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.util.Try

case class AskResponsePayload(
    grid: Option[GridWithDataRenderTypeResponseModel] = None,
    pagination: Option[ChatMessageRequestModel] = None
)

sealed trait GridPropertyDataType

case class GridPropertyDataTypeString(dataType: String = "string")   extends GridPropertyDataType
case class GridPropertyDataTypeNumber(dataType: String = "number")   extends GridPropertyDataType
case class GridPropertyDataTypeObject(dataType: String = "object")   extends GridPropertyDataType
case class GridPropertyDataTypeDate(dataType: String = "date")       extends GridPropertyDataType
case class GridPropertyDataTypeBoolean(dataType: String = "boolean") extends GridPropertyDataType

sealed trait GridPropertyValue

case class GridPropertyValueString(value: String)   extends GridPropertyValue
case class GridPropertyValueNumber(value: Double)   extends GridPropertyValue
case class GridPropertyValueBoolean(value: Boolean) extends GridPropertyValue
case class GridPropertyValueInstant(value: Instant) extends GridPropertyValue

case class GridWithDataRenderTypeResponseModel(
    properties: List[GridPropertyItemModel] = List.empty,
    items: List[Map[String, GridPropertyValue]] = List.empty,
    total: Long = 0L
)

case class GridPropertyItemModel(
    key: String,
    title: String,
    dataType: GridPropertyDataType
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
    case _ @GridPropertyDataTypeObject(dataType)  => dataType.asJson
    case _ @GridPropertyDataTypeDate(dataType)    => dataType.asJson
    case _ @GridPropertyDataTypeBoolean(dataType) => dataType.asJson
  }

  implicit val decodeGridPropertyDataType: Decoder[GridPropertyDataType] =
    Decoder[String].map {
      case "number"  => GridPropertyDataTypeNumber()
      case "object"  => GridPropertyDataTypeObject()
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

object GridPropertyValue extends TimestampCodec {

  val nullValueDescription: String = "нет данных"

  implicit val encodeGridPropertyFilterValue: Encoder[GridPropertyValue] = Encoder.instance {
    case _ @GridPropertyValueString(string)   => string.asJson
    case _ @GridPropertyValueNumber(number)   => number.asJson
    case _ @GridPropertyValueBoolean(boolean) => boolean.asJson
    case _ @GridPropertyValueInstant(instant) => instant.asJson
  }

  implicit val decodeGridPropertyFilterValue: Decoder[GridPropertyValue] =
    Decoder[Instant]
      .map[GridPropertyValue](GridPropertyValueInstant)
      .or(Decoder[Boolean].map[GridPropertyValue](GridPropertyValueBoolean))
      .or(Decoder[Double].map[GridPropertyValue](GridPropertyValueNumber))
      .or(Decoder[String].map[GridPropertyValue](GridPropertyValueString))

  implicit val schema: Schema[GridPropertyValue] = Schema.derived

  def fromValueAndType(value: String, attrType: String): GridPropertyValue = attrType match {
    case "double" | "long" =>
      value.toDoubleOption.fold[GridPropertyValue](GridPropertyValueString(value))(GridPropertyValueNumber)
    case "boolean"         =>
      { value match { case "t" => "True"; case "f" => "False"; case _ => value } }.toBooleanOption
        .fold[GridPropertyValue](GridPropertyValueString(value))(GridPropertyValueBoolean)
    case "datetime"        =>
      Try(Timestamp.valueOf(value).toInstant).toOption
        .fold[GridPropertyValue](GridPropertyValueString(value))(GridPropertyValueInstant)
    case _                 => GridPropertyValueString(value)
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

object AskResponsePayload    {
  implicit val codec: Codec[AskResponsePayload]   = deriveCodec
  implicit val schema: Schema[AskResponsePayload] = Schema.derived
}
