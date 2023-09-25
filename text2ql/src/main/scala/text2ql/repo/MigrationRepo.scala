package text2ql.repo

import cats.effect.kernel._
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.Transactor
import org.typelevel.log4cats.Logger
import text2ql.migration._
import fs2.Stream

import java.util.UUID
import java.time.Instant
import scala.util.Random

trait MigrationRepo[F[_]] {
  def generateEmployees(n: Int): F[Unit]
  def getHrRegionStream: Stream[F, Region]
  def getHrCitiesStream: Stream[F, City]
  def getHrLocationsStream: Stream[F, Location]
  def getHrDepartmentsStream: Stream[F, Department]
  def getHrEmployeesStream: Stream[F, Employee]
  def getHrJobsStream: Stream[F, Job]
  def getHrJobFunctionsStream: Stream[F, JobFunction]
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

    val chunkedEmployees = fs2.Stream.range(11, 11 + n).covary[F].chunkN(1000).map { chunk =>
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

  override def getHrCitiesStream: Stream[F, City] = {
    val sql = "select * from hr.cities"
    Fragment(sql, List.empty).query[City].stream.transact(xaStorage)
  }

  override def getHrLocationsStream: Stream[F, Location] = {
    val sql = "select * from hr.locations"
    Fragment(sql, List.empty).query[Location].stream.transact(xaStorage)
  }

  override def getHrDepartmentsStream: Stream[F, Department] = {
    val sql = "select * from hr.departments"
    Fragment(sql, List.empty).query[Department].stream.transact(xaStorage)
  }

  override def getHrEmployeesStream: Stream[F, Employee] = {
    val sql = "select * from hr.employees"
    Fragment(sql, List.empty).query[Employee].stream.transact(xaStorage)
  }

  override def getHrJobsStream: Stream[F, Job] = {
    val sql = "select * from hr.jobs"
    Fragment(sql, List.empty).query[Job].stream.transact(xaStorage)
  }

  override def getHrJobFunctionsStream: Stream[F, JobFunction] = {
    val sql = "select * from hr.job_functions"
    Fragment(sql, List.empty).query[JobFunction].stream.transact(xaStorage)
  }

}
