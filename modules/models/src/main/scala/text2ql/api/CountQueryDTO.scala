package text2ql.api

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.scalaland.chimney.dsl.TransformerOps
import sttp.tapir.Schema

case class CountQueryDTO(
    hash: Int,
    domain: Domain,
    countRecords: Long,
    countTarget: Option[Long],
    countGroups: Option[Long],
    query: String,
    numberOfUses: Int
)

case class CountQueryResult(
    countRecords: Long,
    countTarget: Option[Long],
    countGroups: Option[Long]
)

case class CountQueryResultUnique(
    countRecords: Long,
    countTarget: Option[Long]
) { self =>

  def toCountQueryResult: CountQueryResult = self
    .into[CountQueryResult]
    .withFieldConst(_.countGroups, None)
    .transform
}

object CountQueryDTO {
  implicit val codec: Codec[CountQueryDTO]   = deriveCodec
  implicit val schema: Schema[CountQueryDTO] = Schema.derived
}
