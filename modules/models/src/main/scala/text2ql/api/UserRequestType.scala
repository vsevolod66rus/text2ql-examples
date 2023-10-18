package text2ql.api

import enumeratum.EnumEntry.Snakecase
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.Codec
import org.http4s.QueryParamEncoder
import sttp.tapir.Schema
import sttp.tapir.codec.enumeratum.TapirCodecEnumeratum

sealed trait UserRequestType extends EnumEntry with Snakecase

object UserRequestType extends Enum[UserRequestType] with TapirCodecEnumeratum with CirceEnum[UserRequestType] {

  case object Undefined                   extends UserRequestType
  case object GetInstanceList             extends UserRequestType
  case object CountInstancesInGroups      extends UserRequestType

  override def values: IndexedSeq[UserRequestType] = findValues

  implicit lazy val config: Configuration = Configuration.default.withSnakeCaseConstructorNames

  implicit val codec: Codec[UserRequestType] = deriveEnumerationCodec

  implicit val schema: Schema[UserRequestType] = Schema.derived

  implicit val queryParamEncoder: QueryParamEncoder[UserRequestType] =
    QueryParamEncoder[String].contramap(e => e.entryName)

}
