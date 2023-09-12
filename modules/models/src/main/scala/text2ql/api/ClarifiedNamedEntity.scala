package text2ql.api

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.Schema

case class ClarifiedNamedEntity(
    tag: String,
    originalValue: String,
    start: Int,
    namedValues: List[String],
    attributeSelected: Option[String],
    fullTextItems: List[FullTextItem] = List.empty[FullTextItem],
    role: Option[String] = None,
    group: Option[Int] = None,
    isTarget: Boolean = false
) {
  def getFirstNamedValue: String = namedValues.headOption.getOrElse("values field is empty")

  def findFirstNamedValue: Option[String] = namedValues.headOption

  def filterByTagAndValueOpt: (String, Option[String]) => Boolean = (name, value) =>
    tag == name && value.contains(originalValue)

  def filterByGroupOpt: Option[Int] => Boolean = filter => group.exists(filter.contains)

  def filterByRole: String => Boolean = filter => role.contains(filter)
}

object ClarifiedNamedEntity {
  implicit val codec: Codec[ClarifiedNamedEntity]   = deriveCodec
  implicit val schema: Schema[ClarifiedNamedEntity] = Schema.derived
}
