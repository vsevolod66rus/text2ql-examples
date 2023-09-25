package text2ql.service

import cats.effect.Async
import cats.effect.kernel._
import com.vaticle.typeql.lang.TypeQL
import com.vaticle.typeql.lang.query.TypeQLInsert
import text2ql.api.Domain
import text2ql.repo.MigrationRepo
import text2ql.typedb.TypeDBTransactionManager
import cats.implicits._
import org.typelevel.log4cats.Logger

import java.util.UUID

trait MigrationService[F[_]] {
  def generateEmployees(n: Int): F[Unit]

  def insertHrToTypeDB: F[Unit]
}

object MigrationService {

  def apply[F[_]: Async: Logger](
      migrationRepo: MigrationRepo[F],
      transactionManager: TypeDBTransactionManager[F]
  ): Resource[F, MigrationService[F]] =
    Resource.eval(Sync[F].delay(new MigrationServiceImpl(migrationRepo, transactionManager)))
}

class MigrationServiceImpl[F[_]: Async: Logger](
    migrationRepo: MigrationRepo[F],
    transactionManager: TypeDBTransactionManager[F]
) extends MigrationService[F] {

  override def generateEmployees(n: Int): F[Unit] = migrationRepo.generateEmployees(n)

  override def insertHrToTypeDB: F[Unit] = for {
    _ <- insertRegionsTypeDB()
  } yield ()

  private def insertRegionsTypeDB() = migrationRepo.getHrRegionStream
    .chunkN(1000)
    .parEvalMap(1) { chunk =>
      transactionManager.write(UUID.randomUUID(), Domain.HR).use { transaction =>
        for {
          _ <- Async[F]
                 .delay {
                   chunk
                     .map { r =>
                       val insertQueryStr =
                         s"""insert $$region isa region, has id "${r.id}", has code "${r.code}", has name "${r.name}";"""
                       val insertQuery    = TypeQL.parseQuery[TypeQLInsert](insertQueryStr)
                       transaction.query().insert(insertQuery)
                     }
                 }
          _  = transaction.commit()
          _  = transaction.close()
          _ <- Logger[F].info(s"inserted ${chunk.size} regions")
        } yield ()
      }
    }
    .compile
    .drain

}
