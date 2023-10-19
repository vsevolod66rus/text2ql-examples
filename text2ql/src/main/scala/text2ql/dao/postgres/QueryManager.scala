package text2ql.dao.postgres

import cats.effect.kernel._
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.util.stream.repeatEvalChunks
import fs2.Stream
import fs2.Stream.{bracket, eval}
import org.typelevel.log4cats.Logger
import text2ql.api._
import text2ql.dao.postgres.QueryManager.GenericPgRow

import java.sql.{PreparedStatement, ResultSet}
import scala.annotation.tailrec

trait QueryManager[F[_]] {
  def getGeneralQueryDTO(queryStr: String): F[GeneralQueryDTO]
  def getCount(buildQueryDTO: BuildQueryDTO, domain: Domain): F[CountQueryDTO]
  def getGenericPgRowStream(query: String, chunkSize: Int = 256): Stream[F, GenericPgRow]
}

object QueryManager {

  type GenericPgRow = Map[String, String]

  def apply[F[_]: Sync: Logger](
      xaStorage: Transactor[F]
  ): Resource[F, QueryManager[F]] =
    Resource.eval(Sync[F].delay(new QueryManagerImpl(xaStorage)))
}

class QueryManagerImpl[F[_]: Sync: Logger](
    xaStorage: Transactor[F]
) extends QueryManager[F] {

  override def getGeneralQueryDTO(queryStr: String): F[GeneralQueryDTO] =
    Fragment(queryStr, List.empty).execWith(exec).transact(xaStorage)

  override def getCount(buildQueryDTO: BuildQueryDTO, domain: Domain): F[CountQueryDTO] = {
    val queryHash = buildQueryDTO.countQuery.hashCode
    for {
      res <- insertCountQueryHash(buildQueryDTO.countQuery, queryHash, domain, buildQueryDTO.aggregation)
    } yield res
  }

  private def exec: PreparedStatementIO[GeneralQueryDTO] =
    for {
      md   <- HPS.executeQuery(FRS.getMetaData)
      cols  = Vector.range(1, md.getColumnCount + 1)
      data <- HPS.executeQuery(readAll(cols))
    } yield GeneralQueryDTO(cols.map(md.getColumnName), data)

  private def readAll(cols: Vector[Int]): ResultSetIO[List[Vector[String]]] =
    readOne(cols).whileM[List](HRS.next)

  private def readOne(cols: Vector[Int]): ResultSetIO[Vector[String]] =
    cols.traverse(i => FRS.getString(i))

  override def getGenericPgRowStream(query: String, chunkSize: Int = 256): Stream[F, GenericPgRow] =
    liftProcessGeneric(chunkSize, FC.prepareStatement(query), ().pure[PreparedStatementIO], FPS.executeQuery)
      .transact(xaStorage)

  private def liftProcessGeneric(
      chunkSize: Int,
      create: ConnectionIO[PreparedStatement],
      prep: PreparedStatementIO[Unit],
      exec: PreparedStatementIO[ResultSet]
  ): Stream[ConnectionIO, GenericPgRow] = {

    def getNextChunkGeneric(chunkSize: Int): ResultSetIO[Seq[GenericPgRow]] = {
      def getChunk(rs: ResultSet, ks: List[String]): Vector[GenericPgRow] = {
        @tailrec
        def iterate(num: Int, result: Vector[GenericPgRow]): Vector[GenericPgRow] = if (num <= chunkSize && rs.next) {
          val row = ks.map(k => k -> Option(rs.getString(k)).getOrElse("Нет данных")).toMap
          iterate(num + 1, result.appended(row))
        } else result

        iterate(0, Vector.empty)
      }

      FRS.raw { rs =>
        val md = rs.getMetaData
        val ks = List.range(1, md.getColumnCount + 1).map(md.getColumnLabel)
        getChunk(rs, ks)
      }
    }

    def prepared(ps: PreparedStatement): Stream[ConnectionIO, PreparedStatement] =
      eval[ConnectionIO, PreparedStatement] {
        val fs = FPS.setFetchSize(chunkSize)
        FC.embed(ps, fs *> prep).map(_ => ps)
      }

    def unrolled(rs: ResultSet): Stream[ConnectionIO, GenericPgRow] =
      repeatEvalChunks(FC.embed(rs, getNextChunkGeneric(chunkSize)))

    val preparedStatement: Stream[ConnectionIO, PreparedStatement] =
      bracket(create)(FC.embed(_, FPS.close)).flatMap(prepared)

    def results(ps: PreparedStatement): Stream[ConnectionIO, GenericPgRow] =
      bracket(FC.embed(ps, exec))(FC.embed(_, FRS.close)).flatMap(unrolled)

    preparedStatement.flatMap(results)

  }

  private def insertCountQueryHash(query: String, hash: Int, domain: Domain, aggregation: Boolean): F[CountQueryDTO] =
    for {
      _            <- Logger[F].info(s"try new count query: $query")
      fr            = Fragment(query, List.empty)
      count        <- Sync[F].blocking {
                        if (aggregation) fr.query[CountQueryResult].unique.transact(xaStorage)
                        else fr.query[CountQueryResultUnique].unique.transact(xaStorage).map(_.toCountQueryResult)
                      }.flatten
      countQueryDTO = CountQueryDTO(
                        hash = hash,
                        domain = domain,
                        countRecords = count.countRecords,
                        countTarget = count.countTarget,
                        countGroups = count.countGroups,
                        query = query,
                        numberOfUses = 0
                      )
    } yield countQueryDTO

}
