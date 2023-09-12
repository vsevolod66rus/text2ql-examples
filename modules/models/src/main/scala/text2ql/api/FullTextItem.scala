package text2ql.api

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

case class FullTextItem(
    value: String,
    score: Double,
    sentences: List[String]
)

object FullTextItem {
  implicit val codec: Codec[FullTextItem]   = deriveCodec
  implicit val schema: Schema[FullTextItem] = Schema.derived
}
