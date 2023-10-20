package text2ql.service

import cats.implicits._
import cats.effect._
import fs2.text
import text2ql.api.Domain
import text2ql.domainschema.{DomainSchemaDTO, DomainSchemaEdge, DomainSchemaVertex}
import text2ql.error.ServerError.ServerErrorWithMessage
import io.circe.yaml.{parser => yamlParser}

trait DomainSchemaService[F[_]] {
  def uploadActive(domain: Domain, contentStream: fs2.Stream[F, Byte]): F[Unit]
  def update(domain: Domain, domainSchemaContent: String): F[Unit]
  def toSchema: PartialFunction[Domain, DomainSchema[F]]
  def vertices(domain: Domain): F[Map[String, DomainSchemaVertex]]
  def schemaAttributesType(domain: Domain): F[Map[String, String]]
  def attributesTitle(domain: Domain): F[Map[String, String]]
  def sqlNames(domain: Domain, key: String): F[String]
  def sqlNamesMap(domain: Domain): F[Map[String, String]]
  def edges(domain: Domain): F[List[DomainSchemaEdge]]
  def thingKeys(domain: Domain): F[Map[String, String]]
  def thingTitle(originalName: String, domain: Domain): F[String]
  def thingKeysSQL(domain: Domain): F[Map[String, String]]
  def thingAttributes(domain: Domain): F[Map[String, Set[String]]]
  def getAttributesByThing(domain: Domain)(thingName: String): F[List[String]]
  def getThingByAttribute(domain: Domain)(attrName: String): F[String]
}

object DomainSchemaService {

  def apply[F[_]: Async]: Resource[F, DomainSchemaService[F]] = for {
    domainSchemaHR <- DomainSchema[F](Domain.HR)
  } yield new DomainSchemaServiceImpl(domainSchemaHR)

  val attributesPriorityForQueryDataCalculating: Map[String, Int] = Map(
    "relation_type"  -> 1,
    "entity_type"    -> 2,
    "attribute_type" -> 3
  )

  val dateAttributes: Set[String] = Set(
    "date",
    "year",
    "month",
    "day",
    "weekday",
    "datetimeTarget"
  )

  val A_TYPE  = "attribute_type"
  val A_VALUE = "attribute_value"
  val E_TYPE  = "entity_type"
  val R_TYPE  = "relation_type"
  val CO      = "comparison_operator"

  val TARGET      = "target"
  val AGGREGATION = "aggregation"
  val ARGUMENT    = "argument"

  val EXTREMUM = "extremum"
  val STATS    = "stats"
  val CHART    = "chart"

  val functionEntities: List[String] = List(EXTREMUM, STATS, CHART)

  val slotsDoNotClarify: List[String] = List(A_VALUE, CO)

  lazy val defaultAttributesTitle: Map[String, String] = Map(
    "entity_type"         -> "Наименование сущности",
    "relation_type"       -> "Наименование связи",
    "attribute_type"      -> "Наименование атрибута",
    "attribute_value"     -> "Значение атрибута",
    "comparison_operator" -> "Оператор сравнения",
    "exist_confirmation"  -> "Подтверждено существование",
    "metric"              -> "Метрика",
    "date"                -> "Дата",
    "day"                 -> "День",
    "weekday"             -> "День недели",
    "month"               -> "Месяц",
    "year"                -> "Год",
    "from"                -> "С",
    "to"                  -> "По",
    "date_unit"           -> "Атрибут даты",
    "counting"            -> "Количество",
    "percent"             -> "Процент",
    "aggregation"         -> "Значение",
    "True"                -> "Да",
    "False"               -> "Нет",
    "true"                -> "Да",
    "false"               -> "Нет",
    "count"               -> "Количество",
    "extremum"            -> "Минимальное или максимальное",
    "week"                -> "неделя",
    "ranking"             -> "рейтинг",
    "stats"               -> "Статистика",
    "attribute_stats"     -> "Статистика по атрибуту",
    "max"                 -> "Максимальное",
    "min"                 -> "Минимальное",
    "chart"               -> "Диаграмма",
    "bar"                 -> "Столбчатая диаграмма"
  )
}

class DomainSchemaServiceImpl[F[_]: Async](
    domainSchemaHR: DomainSchema[F]
) extends DomainSchemaService[F] {

  def toSchema: PartialFunction[Domain, DomainSchema[F]] = { case Domain.HR =>
    domainSchemaHR
  }

  def uploadActive(domain: Domain, contentStream: fs2.Stream[F, Byte]): F[Unit] = for {
    content      <- contentStream.through(text.utf8.decode).compile.string
    domainSchema <- createAndActive(domain, content)
  } yield domainSchema

  private def createAndActive(domain: Domain, content: String): F[Unit] = for {
    _ <- Sync[F]
           .fromEither(yamlParser.parse(content).flatMap(_.as[DomainSchemaDTO]))
           .adaptError(e => ServerErrorWithMessage(e.getMessage))
    _ <- update(domain, content)
  } yield ()

  def update(domain: Domain, domainSchemaContent: String): F[Unit] = toSchema(domain).update(domainSchemaContent)

  def vertices(domain: Domain): F[Map[String, DomainSchemaVertex]] = toSchema(domain).vertices

  def schemaAttributesType(domain: Domain): F[Map[String, String]] = toSchema(domain).schemaAttributesType

  def attributesTitle(domain: Domain): F[Map[String, String]] = toSchema(domain).attributesTitle

  def sqlNames(domain: Domain, key: String): F[String] =
    toSchema(domain).sqlNames.map(_.getOrElse(key, key))

  def sqlNamesMap(domain: Domain): F[Map[String, String]] =
    toSchema(domain).sqlNames

  def edges(domain: Domain): F[List[DomainSchemaEdge]] = toSchema(domain).edges

  def thingKeys(domain: Domain): F[Map[String, String]] = toSchema(domain).thingKeys

  def thingTitle(originalName: String, domain: Domain): F[String] =
    attributesTitle(domain).map(_.getOrElse(originalName, originalName))

  def thingKeysSQL(domain: Domain): F[Map[String, String]] = toSchema(domain).thingKeysSQL

  def thingAttributes(domain: Domain): F[Map[String, Set[String]]] = toSchema(domain).thingAttributes

  def getAttributesByThing(domain: Domain)(thingName: String): F[List[String]] =
    toSchema(domain).thingAttributes.map(_.getOrElse(thingName, Set.empty).toList)

  def getThingByAttribute(domain: Domain)(attrName: String): F[String] =
    toSchema(domain).thingAttributes.map(
      _.find { case (_, v) => v.contains(attrName) }.map(_._1).getOrElse(s"Thing for $attrName not found")
    )
}
