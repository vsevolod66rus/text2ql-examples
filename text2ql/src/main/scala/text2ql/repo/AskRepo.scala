package text2ql.repo

import cats.effect.{Async, Resource}
import cats.implicits._
import fs2.Stream
import org.typelevel.log4cats.Logger
import text2ql.{QueryBuilder, QueryManager}
import text2ql.api.{AskRepoResponse, DataForDBQuery, GridPropertyFilterValue}

trait AskRepo[F[_]] {
  def generalQuery(queryData: DataForDBQuery): F[AskRepoResponse]
  def streamQuery(queryData: DataForDBQuery): F[Stream[F, Map[String, GridPropertyFilterValue]]]
}

object AskRepo {

  def apply[F[_]: Async: Logger](
      qb: QueryBuilder[F],
      qm: QueryManager[F],
      responseBuilder: AskResponseBuilder[F]
  ): Resource[F, AskRepo[F]] =
    Resource.eval(Async[F].delay(new AskRepoImpl[F](qb, qm, responseBuilder)))
}

class AskRepoImpl[F[_]: Async: Logger](
    qb: QueryBuilder[F],
    qm: QueryManager[F],
    responseBuilder: AskResponseBuilder[F]
) extends AskRepo[F] {

  override def generalQuery(queryData: DataForDBQuery): F[AskRepoResponse] = for {
    constantSqlChunk      <- qb.buildConstantSqlChunk(queryData)
    buildQueryDTO         <- qb.buildGeneralSqlQuery(queryData, constantSqlChunk)
    countDTO              <- queryData.count.fold(qm.getCount(buildQueryDTO, queryData.domain))(c => c.pure[F])
    _                     <- Logger[F].info(s"try SQL: ${buildQueryDTO.generalQuery}")
    generalQueryDTO       <- qm.getGeneralQueryDTO(buildQueryDTO.generalQuery)
    res                   <- responseBuilder.buildResponse(queryData, buildQueryDTO, generalQueryDTO, countDTO)
  } yield res

  override def streamQuery(queryData: DataForDBQuery): F[Stream[F, Map[String, GridPropertyFilterValue]]] = for {
    constantSqlChunk <- qb.buildConstantSqlChunk(queryData)
    query            <- qb.buildGeneralSqlQuery(queryData, constantSqlChunk).map(_.generalQuery)
    props            <- responseBuilder.makeGridProperties(queryData)
    res               = qm.getGenericPgRowStream(query)
                          .evalMap { pgRow =>
                            val headers = pgRow.keys.toVector
                            val row     = pgRow.values.toVector
                            responseBuilder.toItem(headers, props, queryData.domain)(row)
                          }
  } yield res

}
