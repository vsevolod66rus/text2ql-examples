package text2ql.repo

import cats.effect.kernel._
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.Transactor
import org.typelevel.log4cats.Logger
import text2ql.migration.{Employee, Region}
import fs2.Stream

import java.util.UUID
import java.time.Instant
import scala.util.Random

trait MigrationRepo[F[_]] {
  def generateEmployees(n: Int): F[Unit]

  def getHrRegionStream: Stream[F, Region]
}

object MigrationRepo {

  def apply[F[_]: Sync: Logger](
      xaStorage: Transactor[F]
  ): Resource[F, MigrationRepo[F]] =
    Resource.eval(Sync[F].delay(new MigrationRepoImpl(xaStorage)))
}

class MigrationRepoImpl[F[_]: Sync: Logger](
    xaStorage: Transactor[F]
) extends MigrationRepo[F] {

  override def generateEmployees(n: Int): F[Unit] = {
    val departmentId = UUID.fromString("aebb311a-527b-11ee-be56-0242ac120009")
    val jobId        = UUID.fromString("5ddd0a88-527c-11ee-be56-0242ac120003")
    val path         = "employee1.employee8" //materialized path иерархия

    val sql = "insert into hr.employees values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    val chunkedEmployees = fs2.Stream.range(12, 12 + n).covary[F].chunkN(1000).map { chunk =>
      chunk.map(i =>
        Employee(
          id = UUID.randomUUID(),
          jobId = jobId,
          departmentId = departmentId,
          gender = if (Random.nextInt() % 2 == 0) true else false,
          name = s"employee$i",
          email = s"employee$i@mail.com",
          hiredDate = Instant.now(),
          fired = false,
          firedDate = None,
          path = s"$path.employee$i"
        )
      )
    }
    chunkedEmployees
      .evalMap { chunk =>
        for {
          insertMany <- Update[Employee](sql)
                          .updateMany(chunk)
                          .transact(xaStorage)
          _          <- Logger[F].info(s"chunk inserted: $insertMany")
        } yield ()
      }
      .compile
      .drain
  }

  override def getHrRegionStream: Stream[F, Region] = {
    val sql = "select * from hr.regions"
    Fragment(sql, List.empty).query[Region].stream.transact(xaStorage)
  }

}
