package text2ql.dao.typedb

import cats.effect.kernel._
import cats.implicits._
import com.vaticle.typedb.client.api.TypeDBTransaction
import com.vaticle.typeql.lang.TypeQL
import com.vaticle.typeql.lang.query.TypeQLMatch
import fs2.Stream
import org.typelevel.log4cats.Logger
import text2ql.api._
import text2ql.configs.TypeDBConfig
import text2ql.error.ServerError.{ServerErrorWithMessage, TypeDBQueryException}

import scala.jdk.CollectionConverters._
import scala.util.Try

trait TypeDBQueryManager[F[_]] {

  def generalQuery(queryData: DataForDBQuery, logic: AggregationLogic, domain: Domain): F[AskRepoResponse]

  def streamQuery(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      readTransaction: TypeDBTransaction,
      domain: Domain
  ): F[Stream[F, Map[String, GridPropertyFilterValue]]]

  def getAttributeValues(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      attrName: String,
      domain: Domain
  ): F[List[GridPropertyFilterValue]]
}

object TypeDBQueryManager {

  def apply[F[_]: Sync: Logger](
      transactionManager: TypeDBTransactionManager[F],
      queryBuilder: TypeDBQueryBuilder[F],
      queryHelper: TypeDBResponseBuilder[F],
      conf: TypeDBConfig
  ): Resource[F, TypeDBQueryManager[F]] =
    Resource.eval(
      Sync[F].delay(new TypeDBQueryManagerImpl(transactionManager, queryBuilder, queryHelper, conf))
    )

}

final class TypeDBQueryManagerImpl[F[_]: Sync: Logger](
    transactionManager: TypeDBTransactionManager[F],
    queryBuilder: TypeDBQueryBuilder[F],
    queryHelper: TypeDBResponseBuilder[F],
    conf: TypeDBConfig
) extends TypeDBQueryManager[F] {

  override def streamQuery(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      readTransaction: TypeDBTransaction,
      domain: Domain
  ): F[Stream[F, Map[String, GridPropertyFilterValue]]] = for {
    query   <- queryBuilder.build(queryData, logic, unlimitedQuery = true, conf.limit, domain)
    getQuery = TypeQL.parseQuery[TypeQLMatch](query)
    _       <- Logger[F].info(s"try typeDB stream Query: $query")
    iterator = readTransaction.query.`match`(getQuery).iterator()

    isSortable                   = (attribute: String) => !attribute.endsWith("_iid")
    raw                          = iterator.asScala.toSeq
    updatedLogic                 = logic.copy(subAttrOpt = None)
    (properties, extractedData) <- queryHelper.makeGridProperties(queryData, raw, updatedLogic, domain, isSortable)

    res = if (logic.unique) {
            // left the chunkSize as 1 to be closer to the previous version implementation
            Stream.fromIterator(iterator = iterator.asScala, chunkSize = 1).map { cm =>
              Map("id" -> GridPropertyFilterValueString(java.util.UUID.randomUUID().toString)) ++
                properties.groupMapReduce(_.key) { prop =>
                  GridPropertyFilterValueString(queryHelper.getCMAttributeStringValue(cm, prop.key))
                }((_, value) => value)
            }
          } else {
            val groupItems = extractedData
              .groupBy(_.aggregationValue)
              .toList
              .sortWith((el1, el2) => el1._2.size > el2._2.size)

            val items = groupItems.map { case (key, value) =>
              Map("id" -> GridPropertyFilterValueString(java.util.UUID.randomUUID().toString)) ++
                properties.groupMapReduce(_.key) {
                  case prop if prop.key == "количество" => GridPropertyFilterValueNumber(value.size.toDouble)
                  case _                                => GridPropertyFilterValueString(value.headOption.map(_.headlineValue).getOrElse(key))
                }((_, value) => value)
            }

            Stream.emits[F, Map[String, GridPropertyFilterValue]](items)
          }
  } yield res

  override def generalQuery(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      domain: Domain
  ): F[AskRepoResponse] = transactionManager
    .read(queryData.requestId, domain)
    .use { readTransaction =>
      val countF = getCount(queryData, logic, readTransaction, domain)
      for {
        count <- countF
        query <- queryBuilder.build(queryData, logic, unlimitedQuery = false, conf.limit, domain)
        _     <- Logger[F].info(s"Try typeDB query: $query")

        getQuery        = TypeQL.parseQuery[TypeQLMatch](query)
        queryResultOpt <-
          Sync[F].delay(Try(readTransaction.query().`match`(getQuery).iterator().asScala.toSeq).toOption)
        queryResult    <- Sync[F].fromOption(queryResultOpt, ServerErrorWithMessage("no result: query error"))

        page    = queryData.pagination.flatMap(_.page).getOrElse(0)
        perPage = queryData.pagination.flatMap(_.perPage).getOrElse(conf.limit)
        offset  = page * perPage
        limit   = perPage
        result <- if (queryResult.nonEmpty) {
                    queryHelper
                      .makeGrid(
                        queryData,
                        queryResult,
                        logic,
                        count.countRecords,
                        domain,
                        conf.nPrimaryColumns,
                        offset,
                        limit
                      )
                      .map { grid =>
                        AskRepoResponse(
                          custom = AskResponsePayload(grid, pagination = queryData.pagination).some,
                          count = count,
                          query = query.some
                        )
                      }
                  } else AskRepoResponse(text = "По Вашему запросу данных не найдено.".some, count = count).pure[F]
      } yield result
    }

  override def getAttributeValues(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      attrName: String,
      domain: Domain
  ): F[List[GridPropertyFilterValue]] = transactionManager
    .read(queryData.requestId, domain)
    .use(getAttributeValuesQuery(queryData, logic, _, attrName, domain))

  private def getAttributeValuesQuery(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      readTransaction: TypeDBTransaction,
      attrName: String,
      domain: Domain
  ): F[List[GridPropertyFilterValue]] = for {
    query    <- queryBuilder.build(queryData, logic, unlimitedQuery = false, 100, domain)
    getQuery <- TypeQL.parseQuery[TypeQLMatch](query).pure[F]
    result   <- readTransaction
                  .query()
                  .`match`(getQuery)
                  .iterator()
                  .asScala
                  .toList
                  .traverse(queryHelper.getCMAttributeValue(_, attrName, domain))
  } yield result

  private def getCount(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      readTransaction: TypeDBTransaction,
      domain: Domain
  ): F[CountQueryDTO] = {
    val updatedEntityList   = queryData.entityList.map { e =>
      val updatedAttributes = e.attributes.collect {
        case a if a.attributeValues.exists(_.nonEmpty) => a.copy(includeGetClause = false)
      }
      e.copy(attributes = updatedAttributes, includeGetClause = true)
    }
    val updatedRelationList = queryData.relationList.map { r =>
      val updatedAttributes =
        r.attributes.collect {
          case a if a.attributeValues.exists(_.nonEmpty) => a.copy(includeGetClause = false)
        }
      r.copy(attributes = updatedAttributes, includeGetClause = false)
    }

    val filteredData = queryData.copy(entityList = updatedEntityList, relationList = updatedRelationList)
    val countClause  = "count;"

    def getQuery(desc: Boolean = false) = queryBuilder.build(
      filteredData,
      logic,
      unlimitedQuery = true,
      conf.limit,
      domain,
      countQuery = true,
      countDistinctQuery = desc
    )

    for {
      queryCountGeneral <- getQuery().map(_ + countClause)
      countGeneral      <- getCountQueryResult(queryCountGeneral, domain, readTransaction)
      queryCountTarget  <- getQuery(desc = true).map(_ + countClause)
      countTarget       <- getCountQueryResult(queryCountTarget, domain, readTransaction)
    } yield CountQueryDTO(
      hash = countGeneral.hash,
      domain = domain,
      countRecords = countGeneral.countRecords,
      countTarget = countTarget.countRecords.some,
      countGroups = None,
      query = countGeneral.query,
      numberOfUses = countTarget.numberOfUses
    )
  }

  private def getCountQueryResult(
      query: String,
      domain: Domain,
      readTransaction: TypeDBTransaction
  ): F[CountQueryDTO] = {
    val queryHash = query.hashCode
    for {
      res <- insertCountQueryHash(query, readTransaction, queryHash, domain)
    } yield res
  }

  private def insertCountQueryHash(
      query: String,
      readTransaction: TypeDBTransaction,
      hash: Int,
      domain: Domain
  ): F[CountQueryDTO] = for {
    typeQLQuery <- Sync[F]
                     .delay(TypeQL.parseQuery[TypeQLMatch.Aggregate](query))
    countOpt    <- Sync[F].delay(Try(readTransaction.query.`match`(typeQLQuery).get().asLong()).toOption)
    count       <- Sync[F].fromOption(countOpt, ServerErrorWithMessage("no result: count query error"))
    countDTO     = CountQueryDTO(
                     hash = hash,
                     domain = domain,
                     countRecords = count,
                     countTarget = None,
                     countGroups = None,
                     query = query,
                     numberOfUses = 0
                   )
  } yield countDTO

}
