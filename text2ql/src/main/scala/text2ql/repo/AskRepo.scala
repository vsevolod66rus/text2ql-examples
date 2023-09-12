package text2ql.repo

import cats.effect.{Async, Resource}
import cats.implicits._
import fs2.Stream
import org.typelevel.log4cats.Logger
import text2ql.{QueryBuilder, QueryManager, UnloggedTableManager}
import text2ql.api.{AskRepoResponse, DataForDBQuery, GridPropertyFilterValue}

trait AskRepo[F[_]] {
  def generalQuery(queryData: DataForDBQuery): F[AskRepoResponse]
  def getAttributeValues(queryData: DataForDBQuery, attributeName: String): F[List[GridPropertyFilterValue]]
  def streamQuery(queryData: DataForDBQuery): F[Stream[F, Map[String, GridPropertyFilterValue]]]
  def createUnloggedTables(queryData: DataForDBQuery): F[Unit]
  def deleteUnloggedTables(queryData: DataForDBQuery): F[Unit]
}

object AskRepo {

  def apply[F[_]: Async: Logger](
      qb: QueryBuilder[F],
      qm: QueryManager[F],
      utm: UnloggedTableManager[F],
      responseBuilder: AskResponseBuilder[F]
  ): Resource[F, AskRepo[F]] =
    Resource.eval(Async[F].delay(new AskRepoImpl[F](qb, qm, utm, responseBuilder)))
}

class AskRepoImpl[F[_]: Async: Logger](
    qb: QueryBuilder[F],
    qm: QueryManager[F],
    utm: UnloggedTableManager[F],
    responseBuilder: AskResponseBuilder[F]
) extends AskRepo[F] {

  override def generalQuery(queryData: DataForDBQuery): F[AskRepoResponse] = for {
    constantSqlChunk      <- qb.buildConstantSqlChunk(queryData)
    isFulltextAttrs        = isFullText(queryData: DataForDBQuery)
    _                     <- createUnloggedTables(queryData).whenA(isFulltextAttrs)
    buildQueryDTO         <- qb.buildGeneralSqlQuery(queryData, constantSqlChunk)
    countDTO              <- queryData.count.fold(qm.getCount(buildQueryDTO, queryData.domain))(c => c.pure[F])
    _                     <- Logger[F].info(s"try SQL: ${buildQueryDTO.generalQuery}")
    generalQueryDTO       <- qm.getGeneralQueryDTO(buildQueryDTO.generalQuery)
    _                     <- deleteUnloggedTables(queryData).whenA(isFulltextAttrs)
    numericDescriptionOpt <-
      buildQueryDTO.numericDescriptionQuery
        .filter(_ => countDTO.countTarget.exists(_ > 0))
        .traverse(qm.getNumericDescription)
    res                   <- responseBuilder.buildResponse(queryData, buildQueryDTO, generalQueryDTO, countDTO, numericDescriptionOpt)
  } yield res

  override def getAttributeValues(
      queryData: DataForDBQuery,
      attributeName: String
  ): F[List[GridPropertyFilterValue]] = for {
    constantSqlChunk <- qb.buildConstantSqlChunk(queryData)
    query            <- qb.buildGeneralSqlQuery(queryData, constantSqlChunk).map(_.generalQuery)
    _                <- Logger[F].info(s"try SQL for attribute values: $query")
    generalQueryDTO  <- qm.getGeneralQueryDTO(query)
    result           <- generalQueryDTO.data
                          .traverse(responseBuilder.getAttributeValue(generalQueryDTO.headers, attributeName, queryData.domain))
  } yield result

  override def streamQuery(queryData: DataForDBQuery): F[Stream[F, Map[String, GridPropertyFilterValue]]] = for {
    constantSqlChunk <- qb.buildConstantSqlChunk(queryData)
    isFulltextAttrs   = isFullText(queryData: DataForDBQuery)
    _                <- utm.createUnloggedTables(queryData).whenA(isFulltextAttrs)
    query            <- qb.buildGeneralSqlQuery(queryData, constantSqlChunk).map(_.generalQuery)
    props            <- responseBuilder.makeGridProperties(queryData)
    res               = qm.getGenericPgRowStream(query)
                          .evalMap { pgRow =>
                            val headers = pgRow.keys.toVector
                            val row     = pgRow.values.toVector
                            responseBuilder.toItem(headers, props, queryData.domain)(row)
                          }
                          .onFinalize(utm.deleteUnloggedTables(queryData).whenA(isFulltextAttrs))
  } yield res

  override def createUnloggedTables(queryData: DataForDBQuery): F[Unit] =
    utm.createUnloggedTables(queryData).whenA(isFullText(queryData))

  override def deleteUnloggedTables(queryData: DataForDBQuery): F[Unit] =
    utm.deleteUnloggedTables(queryData).whenA(isFullText(queryData))

  private def isFullText(queryData: DataForDBQuery): Boolean =
    (queryData.entityList.flatMap(_.attributes) ++ queryData.relationList.flatMap(_.attributes))
      .exists(_.fullTextItems.nonEmpty)
}
