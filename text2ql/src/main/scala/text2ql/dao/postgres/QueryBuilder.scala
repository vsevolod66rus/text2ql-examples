package text2ql.dao.postgres

import cats.effect.kernel.{Resource, Sync}
import cats.implicits._
import text2ql.api._
import text2ql.configs.DBDataConfig
import text2ql.domainschema.DomainSchemaEdge
import text2ql.service.DomainSchemaService
import text2ql.service.DomainSchemaService._

import java.util.UUID
import scala.annotation.tailrec

trait QueryBuilder[F[_]] {
  def buildConstantSqlChunk(queryData: DataForDBQuery): F[String]
  def buildGeneralSqlQuery(queryData: DataForDBQuery, constantSqlChunk: String): F[BuildQueryDTO]
  def buildNumericValueQuery(attributeName: String, constantSqlChunk: String): F[String]
}

object QueryBuilder {

  def apply[F[_]: Sync](
      domainSchema: DomainSchemaService[F],
      dataConf: DBDataConfig
  ): Resource[F, QueryBuilder[F]] =
    Resource.eval(Sync[F].delay(new QueryBuilderImpl(domainSchema, dataConf)))
}

class QueryBuilderImpl[F[_]: Sync](
    domainSchema: DomainSchemaService[F],
    dataConf: DBDataConfig
) extends QueryBuilder[F] {

  override def buildConstantSqlChunk(queryData: DataForDBQuery): F[String] = for {
    edges       <- domainSchema.edges(queryData.domain)
    _           <- extractNameWithAttributes(queryData).fold("".pure[F]) { case (name, attributes) =>
                     buildThingChunk(name, attributes, queryData.domain, queryData.requestId)
                   }
    targetEntity = findTargetEntity(queryData)
    pe          <- targetEntity.fold("".pure[F])(e => bfs("".pure[F], List(e), Set.empty[String], queryData, edges))
    attributes   =
      queryData.entityList.flatMap(e => e.attributes.map((e.entityName, _))) ++
        queryData.relationList.flatMap(r => r.attributes.map((r.relationName, _)))
    filters     <- attributes
                     .filter { case (_, attr) => attr.fullTextItems.isEmpty }
                     .filter { case (_, attr) => attr.attributeValues.exists(_.nonEmpty) }
                     .traverse { case (thing, attr) => buildFiltersChunk(queryData.domain, thing)(attr) }
    filtersChunk =
      filters.filter(_.nonEmpty).mkString(" AND ").some.filter(_.nonEmpty).map("WHERE " + _).fold("")(identity)
  } yield List("FROM", pe, filtersChunk).mkString(" ")

  private def extractNameWithAttributes(
      queryData: DataForDBQuery
  ): Option[(String, List[AttributeForDBQuery])] = findTargetEntity(queryData)
    .map(e => (e.entityName, e.attributes.filter(_.attributeValues.nonEmpty)))

  private def findTargetEntity(queryData: DataForDBQuery): Option[EntityForDBQuery] =
    queryData.entityList.find(_.isTargetEntity)

  def buildGeneralSqlQuery(queryData: DataForDBQuery, constantSqlChunk: String): F[BuildQueryDTO] =
    if (queryData.logic.unique) buildSqlForTable(queryData, constantSqlChunk)
    else buildSqlForChart(queryData, constantSqlChunk)

  private def buildSqlForTable(queryData: DataForDBQuery, constantSqlChunk: String): F[BuildQueryDTO] =
    for {
      visualization          <- queryData.logic.visualization.pure[F]
      attributesForMainSelect =
        filterIncludeSelectAttributes(queryData)
          .filter { case (_, attribute) => visualization.tags.contains(attribute.attributeName) }
          .sortBy { case (_, attribute) => visualization.tags.indexOf(attribute.attributeName) }
      mainSelect             <- collectMainSelect(attributesForMainSelect, queryData.domain)
                                  .map(s => if (queryData.logic.distinct) s"distinct $s" else s)
      orderBy                <- buildOrderByChunk(queryData)
      query                   = s"$mainSelect $constantSqlChunk $orderBy"
      generalQuery            = withPagination(query, queryData.pagination)
      countTargetChunk       <- buildCountTargetChunk(queryData)
      countQuery              = buildCountQuery(constantSqlChunk, countTargetChunk, None, None)
    } yield BuildQueryDTO(
      generalQuery = generalQuery,
      countQuery = countQuery,
      aggregation = false
    )

  override def buildNumericValueQuery(attributeName: String, constantSqlChunk: String): F[String] = for {
    maxChunk   <- s"max($attributeName) as max_value,".pure[F]
    minChunk    = s"min($attributeName) as min_value,"
    avgChunk    = s"avg($attributeName) as avg_value,"
    medianChunk = s"percentile_cont(0.5) within group (order by $attributeName) as median_value,"
    sumChunk    = s"sum($attributeName) as sum_value"
  } yield s"select $maxChunk $minChunk $avgChunk $medianChunk $sumChunk $constantSqlChunk;"

  private def buildSqlForChart(queryData: DataForDBQuery, constantSqlChunk: String): F[BuildQueryDTO] = for {
    keys                  <- domainSchema.thingKeys(queryData.domain)
    countingChunk         <-
      buildChunkFromLogicAttrs(queryData.logic.targetAttr, queryData.logic.targetThing, queryData.domain).map { c =>
        queryData.requestType match {
          case _ if queryData.logic.targetThing == queryData.logic.groupByThing => c
          case _                                                                => s"distinct $c"
        }
      }
    aggregationChunk      <- queryData.logic.subAttrOpt.fold {
                               buildChunkFromLogicAttrs(
                                 queryData.logic.groupByAttr,
                                 queryData.logic.groupByThing,
                                 queryData.domain
                               )
                             } { sa =>
                               buildChunkForDateSubAttributes(
                                 queryData.logic.groupByAttr,
                                 queryData.logic.groupByThing,
                                 queryData.domain,
                                 sa
                               )
                             }
    aggregationKey         =
      keys.getOrElse(queryData.logic.groupByThing, s"no key for ${queryData.logic.groupByThing}")
    idChunkCondition       = queryData.logic.groupByAttr == queryData.logic.groupByThing
    idChunk               <- if (idChunkCondition)
                               buildThingIdChunk(aggregationKey, queryData.logic.groupByThing, queryData.domain)
                             else "".pure[F]
    groupByChunk           = if (idChunkCondition) "GROUP BY aggregation, thing_id" else "GROUP BY aggregation"
    percentChunk           = s"count($countingChunk) * 100.0 / sum(count(*)) over () as percent"
    countOrSum             = "count"
    sortDirection          =
      queryData.logic.sortModelOpt.flatMap(_.direction).fold[SortDirection](SortDirection.desc)(identity).entryName
    havingAggregation      = queryData.logic.aggregationFilterOpt.fold("") { f =>
                               s"having count(*) ${f.comparisonOperator} ${f.value}"
                             }
    query                  =
      s"$countOrSum($countingChunk) as counting, $percentChunk, $aggregationChunk as aggregation $idChunk $constantSqlChunk " +
        s"$groupByChunk $havingAggregation ORDER BY counting $sortDirection"
    generalQuery           = withPagination(query, queryData.pagination)
    queryCountTargetChunk <- buildCountTargetChunk(queryData)
    countQuery             =
      buildCountQuery(
        constantSqlChunk,
        queryCountTargetChunk,
        aggregationChunk.some,
        queryData.logic.aggregationFilterOpt
      )
  } yield BuildQueryDTO(
    generalQuery = generalQuery,
    countQuery = countQuery,
    aggregation = true
  )

  private def withPagination(query: String, pagination: Option[ChatMessageRequestModel]): String = {

    def fixedWithCursorBasedPagination(offset: Int, limit: Int): String =
      s"SELECT * FROM (SELECT row_number() over() as cursor, * FROM (SELECT $query)" +
        s" as sorted_res) as res where res.cursor > $offset and res.cursor < ${offset + limit + 1};"

    def withOffsetLimit(offset: Int, limit: Int): String = s"SELECT $query LIMIT $limit OFFSET $offset;"

    val limitOpt  = pagination.flatMap(_.perPage)
    val offsetOpt = pagination.flatMap(_.page).flatMap(o => limitOpt.map(_ * o))
    (offsetOpt, limitOpt) match {
      case (Some(o), Some(l)) if dataConf.useCursor => fixedWithCursorBasedPagination(o, l)
      case (Some(o), Some(l))                       => withOffsetLimit(o, l)
      case _                                        => s"SELECT $query;"
    }
  }

  private def buildChunkFromLogicAttrs(attr: String, thing: String, domain: Domain): F[String] =
    for {
      sqlName <- domainSchema.sqlNames(domain, attr)
      chunk    = s"${thing.toUpperCase}.$sqlName"
    } yield chunk

  private def buildChunkForDateSubAttributes(attr: String, thing: String, domain: Domain, subAttr: String): F[String] =
    buildChunkFromLogicAttrs(attr, thing, domain).map { aggregationChunk =>
      val datePart = dateUnits.getOrElse(subAttr, subAttr)
      s"date_part('$datePart', $aggregationChunk)"
    }

  private def buildThingIdChunk(attr: String, thing: String, domain: Domain): F[String] =
    buildChunkFromLogicAttrs(attr, thing, domain).map(name => s", $name as thing_id")

  private def buildOrderByChunk(queryData: DataForDBQuery): F[String] =
    queryData.pagination
      .map(_.sort)
      .filter(_.orderBy.exists(_.nonEmpty))
      .filter(sortModel => sortModel.orderBy.exists(_.nonEmpty) && sortModel.direction.nonEmpty)
      .fold {
        val keyAttrsF = findTargetEntity(queryData).map(findKeyAttributeOfEntity(queryData.domain)).sequence
        keyAttrsF.map(_.fold("")(key => s"order by $key asc"))
      } { sortModel =>
        s"order by ${sortModel.orderBy.getOrElse("orderBy")} ${sortModel.direction.getOrElse("sortDirection")}".pure[F]
      }

  private def findKeyAttributeOfEntity(domain: Domain)(entity: EntityForDBQuery): F[String] = for {
    thingKeys    <- domainSchema.thingKeys(domain)
    thingKey      = thingKeys.getOrElse(entity.entityName, entity.entityName)
    keyAttribute <- domainSchema
                      .sqlNames(domain, thingKey)
                      .map(name => s"${entity.entityName.toUpperCase}.$name")
  } yield keyAttribute

  private def buildCountQuery(
      constantSqlChunk: String,
      countTargetChunk: String,
      aggregationChunk: Option[String],
      aggregationFilter: Option[AggregationFilter]
  ): String = (aggregationChunk, aggregationFilter) match {
    case (Some(aggregation), Some(filter)) =>
      s"select sum(count_records), sum(count_target), count(aggregation) from " +
        s"(select count(*) as count_records, $countTargetChunk as count_target, $aggregation as aggregation " +
        s"$constantSqlChunk group by aggregation having count(*) ${filter.comparisonOperator} ${filter.value}) res;"
    case (Some(aggregation), None)         =>
      s"select count(*), $countTargetChunk, count(distinct $aggregation) $constantSqlChunk;"
    case _                                 => s"select count(*), $countTargetChunk $constantSqlChunk"
  }

  private def buildCountTargetChunk(queryData: DataForDBQuery): F[String] = {
    val targetThing = queryData.logic.targetThing
    for {
      distinctKey  <- domainSchema.thingKeys(queryData.domain).map(_.getOrElse(targetThing, targetThing))
      sqlNameKey   <- domainSchema.sqlNames(queryData.domain, distinctKey)
      distinctThing = s"${targetThing.toUpperCase}.$sqlNameKey"
    } yield s"COUNT(DISTINCT $distinctThing)"
  }

  private def filterIncludeSelectAttributes(queryData: DataForDBQuery): List[(String, AttributeForDBQuery)] = {
    val fromEntities  =
      queryData.entityList.flatMap(e => e.attributes.filter(_.includeGetClause).map((e.entityName.toUpperCase, _)))
    val fromRelations =
      queryData.relationList.flatMap(r => r.attributes.filter(_.includeGetClause).map((r.relationName.toUpperCase, _)))
    fromEntities ++ fromRelations
  }

  private def collectMainSelect(attributes: List[(String, AttributeForDBQuery)], domain: Domain): F[String] =
    attributes
      .traverse { case (entityName, attr) =>
        domainSchema
          .sqlNames(domain, attr.attributeName)
          .map { name =>
            val pgAttr = attr.datePart.fold(s"$entityName.$name")(dp => s"date_part('$dp',$entityName.$name)")
            s"$pgAttr as ${attr.attributeName}"
          }
      }
      .map(_.mkString(", "))

  private def buildThingChunk(
      thingName: String,
      attributes: List[AttributeForDBQuery],
      domain: Domain,
      requestId: UUID
  ): F[String] = for {
    selectChunk  <- domainSchema.select(domain, thingName).map("SELECT " + _)
    pgSchemaName  = dataConf.pgSchemas.getOrElse(domain, domain.entryName.toLowerCase)
    fromChunk    <- domainSchema.from(domain, thingName, pgSchemaName).map("FROM " + _)
    joinChunkRaw <- domainSchema.join(domain, thingName)
    joinChunk     = joinChunkRaw.map {
                      _.replaceAll("join ", s"join $pgSchemaName.")
                        .replaceAll("JOIN ", s"JOIN $pgSchemaName.")
                    }
    groupByChunk <- domainSchema.groupBy(domain, thingName).map(_.map("GROUP BY " + _))
    havingChunk  <- domainSchema.having(domain, thingName).map(_.map("HAVING " + _))
    orderByChunk <- domainSchema.orderBy(domain, thingName).map(_.map("ORDER " + _))
    whereChunk   <- domainSchema.where(domain, thingName).map(_.map("WHERE " + _))
    res           = List(
                      "(".some,
                      selectChunk.some,
                      fromChunk.some,
                      joinChunk,
                      whereChunk,
                      groupByChunk,
                      havingChunk,
                      orderByChunk,
                      ") as".some,
                      thingName.toUpperCase.some
                    ).collect { case Some(s) => s }.mkString(" ")
  } yield res

  private def buildFiltersChunk(domain: Domain, thingName: String)(attributeQuery: AttributeForDBQuery): F[String] =
    for {
      name        <- domainSchema
                       .sqlNames(domain, attributeQuery.attributeName)
                       .map(name => s"${thingName.toUpperCase}.$name")
      eitherOrAnd <- Sync[F].delay {
                       val isOrExists = attributeQuery.attributeValues.flatMap(_.headOption).exists(_.joinValuesWithOr)
                       if (isOrExists) "OR" else "AND"
                     }
      attrs       <- attributeQuery.attributeValues
                       .getOrElse(List.empty[AttributeValue])
                       .traverse(buildAttributeFilter(domain, attributeQuery, name))
                       .map(_.filter(_.nonEmpty))
      res          = if (attrs.size > 1) {
                       val chunk = attrs.mkString(s") $eitherOrAnd (")
                       s"(($chunk))"
                     } else attrs.mkString(s" $eitherOrAnd ")
    } yield res

  private def buildAttributeFilter(domain: Domain, attributeQuery: AttributeForDBQuery, sqlNameRaw: String)(
      attribute: AttributeValue
  ): F[String] = for {
    attrsTypes <- domainSchema.schemaAttributesType(domain)
    attrType    = attrsTypes.getOrElse(attributeQuery.attributeName, "string")
    sqlName     = if (attrType == "datetime")
                    attributeQuery.datePart.fold(s"$sqlNameRaw::date")(dp => s"date_part('$dp', $sqlNameRaw::date)")
                  else sqlNameRaw
    value       = if (List("string", "datetime").contains(attrType)) s"'${attribute.value}'" else s"${attribute.value}"
    chunk       =
      if (GridPropertyFilterValue.nullValueDescription == value) s"$sqlName is null"
      else s"$sqlName ${attribute.comparisonOperator} $value"
  } yield chunk

  @tailrec
  private def bfs(
      acc: F[String],
      queue: List[EntityForDBQuery],
      visited: Set[String],
      queryData: DataForDBQuery,
      edges: List[DomainSchemaEdge]
  ): F[String] = queue match {
    case head :: tail =>
      val adjacentVertices = edges.collect {
        case e if e.from == head.entityName => e.to
        case e if e.to == head.entityName   => e.from
      }
      val sqlF             = for {
        currentSql  <- acc
        vertexChunk <- buildThingChunk(head.entityName, head.attributes, queryData.domain, queryData.requestId)
        joinOnChunk  = edges
                         .filter(e => Set(e.from, e.to).contains(head.entityName))
                         .filter(e => Set(e.from, e.to).intersect(visited).nonEmpty)
                         .map(e => s"${e.from}.${e.fromKey} = ${e.to}.${e.toKey}")
                         .mkString(" and ")
                         .some
                         .filter(_.nonEmpty)
                         .fold("")(ch => s"on $ch")
        res          =
          if (currentSql.isEmpty) s"$vertexChunk $joinOnChunk" else s"$currentSql inner join $vertexChunk $joinOnChunk"
      } yield res
      bfs(
        sqlF,
        tail ++ queryData.entityList
          .filter(e => adjacentVertices.contains(e.entityName))
          .filterNot(e => visited.contains(e.entityName))
          .filterNot(e => tail.map(_.entityName).contains(e.entityName)),
        visited + head.entityName,
        queryData,
        edges
      )
    case _            => acc
  }
}
