package text2ql.dao.postgres

import cats.effect.{Async, Resource}
import cats.implicits._
import text2ql.api._
import text2ql.service.DomainSchemaService

trait ResponseBuilder[F[_]] {

  def buildResponse(
      queryData: DataForDBQuery,
      buildQueryDTO: BuildQueryDTO,
      generalQueryDTO: GeneralQueryDTO,
      countQueryDTO: CountQueryDTO
  ): F[AskRepoResponse]

  def makeGridProperties(queryData: DataForDBQuery): F[List[GridPropertyItemModel]]

  def toItem(headers: Vector[String], properties: List[GridPropertyItemModel], domain: Domain)(
      row: Vector[String]
  ): F[Map[String, GridPropertyValue]]

  def getAttributeValue(
      headers: Vector[String],
      attribute: String,
      domain: Domain
  )(row: Vector[String]): F[GridPropertyValue]
}

object ResponseBuilder {

  def apply[F[_]: Async](
      domainSchema: DomainSchemaService[F]
  ): Resource[F, ResponseBuilder[F]] =
    Resource.eval(Async[F].delay(new ResponseBuilderImpl[F](domainSchema)))
}

class ResponseBuilderImpl[F[_]: Async](
    domainSchema: DomainSchemaService[F]
) extends ResponseBuilder[F] {

  override def buildResponse(
      queryData: DataForDBQuery,
      buildQueryDTO: BuildQueryDTO,
      generalQueryDTO: GeneralQueryDTO,
      countQueryDTO: CountQueryDTO
  ): F[AskRepoResponse] = {
    val count = countQueryDTO.countGroups.getOrElse(countQueryDTO.countRecords)
    makeGrid(generalQueryDTO, queryData, count).map(toAskResponse(countQueryDTO, buildQueryDTO.generalQuery, queryData))
  }

  override def makeGridProperties(
      queryData: DataForDBQuery
  ): F[List[GridPropertyItemModel]] = if (queryData.logic.unique)
    buildTableProperties(queryData.logic, queryData.domain)
  else buildChartProperties(queryData.logic, queryData.domain)

  override def toItem(headers: Vector[String], properties: List[GridPropertyItemModel], domain: Domain)(
      row: Vector[String]
  ): F[Map[String, GridPropertyValue]]         =
    properties
      .traverse(prop => getAttributeValue(headers, prop.key, domain)(row).map(v => prop.key -> v))
      .map(_.groupMapReduce(_._1)(_._2)((_, v) => v))
      .map(m => Map("id" -> GridPropertyValueString(java.util.UUID.randomUUID().toString)) ++ m)

  override def getAttributeValue(
      headers: Vector[String],
      attribute: String,
      domain: Domain
  )(row: Vector[String]): F[GridPropertyValue] = domainSchema.schemaAttributesType(domain).map { attrs =>
    val stringValue = row(headers.indexOf(attribute))
    val attrType    = if (attribute == "counting") "long" else attrs.getOrElse(attribute, "string")
    GridPropertyValue.fromValueAndType(stringValue, attrType)
  }

  private def toAskResponse(
      count: CountQueryDTO,
      query: String,
      queryData: DataForDBQuery
  )(grid: GridWithDataRenderTypeResponseModel): AskRepoResponse =
    if (count.countRecords == 0)
      AskRepoResponse(text = "По Вашему запросу данных не найдено.".some, count = count, query = query.some)
    else
      AskRepoResponse(
        custom = AskResponsePayload(grid.some, pagination = queryData.pagination).some,
        count = count,
        query = query.some
      )

  private def buildTableProperties(
      logic: AggregationLogic,
      domain: Domain
  ): F[List[GridPropertyItemModel]] = logic.visualization.tags
    .traverse { attribute =>
      for {
        attrType <- domainSchema.schemaAttributesType(domain).map(_.getOrElse(attribute, "string"))
        title    <- domainSchema.attributesTitle(domain).map(_.getOrElse(attribute, attribute))
      } yield GridPropertyItemModel(
        key = attribute,
        title = title,
        dataType = GridPropertyDataType.fromType(attrType)
      )
    }

  private def buildChartProperties(
      logic: AggregationLogic,
      domain: Domain
  ): F[List[GridPropertyItemModel]] = for {
    aggregationOriginalName <- Async[F].delay(logic.groupByThing)
    countingOriginalName     = logic.targetThing
    aggregationTitle        <-
      domainSchema.attributesTitle(domain).map(_.getOrElse(aggregationOriginalName, aggregationOriginalName))
    countingTitle           <-
      domainSchema.attributesTitle(domain).map(_.getOrElse(countingOriginalName, countingOriginalName))
  } yield List(
    GridPropertyItemModel(
      key = "aggregation",
      title = aggregationTitle,
      dataType = GridPropertyDataTypeString()
    ),
    GridPropertyItemModel(
      key = "counting",
      title = s"""Количество экземпяров "$countingTitle"""",
      dataType = GridPropertyDataTypeNumber()
    )
  )

  private def makeGrid(
      generalQueryDTO: GeneralQueryDTO,
      queryData: DataForDBQuery,
      total: Long
  ): F[GridWithDataRenderTypeResponseModel] =
    for {
      properties <- makeGridProperties(queryData)
      items      <- generalQueryDTO.data.traverse(toItem(generalQueryDTO.headers, properties, queryData.domain))
    } yield GridWithDataRenderTypeResponseModel(
      properties = properties,
      items = items,
      total = total
    )

}
