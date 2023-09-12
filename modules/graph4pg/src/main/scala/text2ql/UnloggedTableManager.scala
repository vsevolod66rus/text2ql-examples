package text2ql

import cats.effect.kernel._
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.{Fragment, Transactor}
import org.typelevel.log4cats.Logger
import text2ql.api.{AttributeForDBQueryWithThingName, DataForDBQuery}
import text2ql.service.DomainSchemaService

import java.util.UUID

trait UnloggedTableManager[F[_]] {
  def createUnloggedTables(queryData: DataForDBQuery): F[Unit]
  def deleteUnloggedTables(queryData: DataForDBQuery): F[Unit]
  def makeUnloggedTableName(attributeName: String, requestId: UUID): String
}

object UnloggedTableManager {

  def apply[F[_]: Sync: Logger](
      domainSchema: DomainSchemaService[F],
      xaStorage: Transactor[F]
  ): Resource[F, UnloggedTableManager[F]] =
    Resource.eval(Sync[F].delay(new TempTableManagerImpl(xaStorage, domainSchema)))
}

class TempTableManagerImpl[F[_]: Sync: Logger](
    xaStorage: Transactor[F],
    domainSchema: DomainSchemaService[F]
) extends UnloggedTableManager[F] {

  override def createUnloggedTables(queryData: DataForDBQuery): F[Unit] =
    getFullTextAttrsWithThings(queryData).traverse { dto =>
      for {
        values      <- dto.attribute.fullTextItems.flatMap(_.sentences).distinct.pure[F]
        tableName    = makeUnloggedTableName(dto.attribute.attributeName, queryData.requestId)
        keyName     <- domainSchema.thingKeysSQL(queryData.domain).map(_.getOrElse(dto.thingName, dto.thingName))
        queryStr     = createUnloggedTableSql(tableName, keyName)
        createTable <- Fragment.const(queryStr).update.run.transact(xaStorage)
        _           <- Logger[F].info(s"created unlogged table $tableName: $createTable")
        insertKeys  <-
          Update[String](s"insert into $tableName ($keyName) values (?)")
            .updateMany(values)
            .transact(xaStorage)
        _           <- Logger[F].info(s"inserted into table $tableName: $insertKeys")
      } yield ()
    }.void

  override def deleteUnloggedTables(queryData: DataForDBQuery): F[Unit] = for {
    attrsWithThings <- getFullTextAttrsWithThings(queryData).pure[F]
    tempTables       = attrsWithThings
                         .map(dto => makeUnloggedTableName(dto.attribute.attributeName, queryData.requestId))
    _               <- tempTables.traverse { tableName =>
                         Fragment
                           .const(s"drop table if exists $tableName")
                           .update
                           .run
                           .transact(xaStorage)
                           .flatMap(dropTable => Logger[F].info(s"deleted unlogged table $tableName: $dropTable"))
                       }
  } yield ()

  override def makeUnloggedTableName(attributeName: String, requestId: UUID): String =
    s"${attributeName}_keys_${(requestId.hashCode & Int.MaxValue).toString}"

  private def getFullTextAttrsWithThings(queryData: DataForDBQuery): List[AttributeForDBQueryWithThingName]      =
    (queryData.entityList.map(e => (e.attributes, e.entityName)) ++
      queryData.relationList.map(r => (r.attributes, r.relationName)))
      .flatMap { case (attrs, thingName) => attrs.map((_, thingName)) }
      .filter { case (attr, _) => attr.fullTextItems.nonEmpty }
      .map { case (attr, thingName) => AttributeForDBQueryWithThingName(attr, thingName) }

  private def createUnloggedTableSql(tableName: String, keyName: String, useClickhouse: Boolean = false): String =
    if (useClickhouse) s"create table $tableName ($keyName text) engine Memory"
    else s"create unlogged table $tableName ($keyName text)"

}
