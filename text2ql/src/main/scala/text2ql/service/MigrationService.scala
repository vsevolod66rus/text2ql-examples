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
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

trait MigrationService[F[_]] {
  def generateEmployees(n: Int): F[Unit]
  def insertHrToTypeDB(): F[Unit]
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
    _ <- insertCitiesTypeDB()
    _ <- insertLocationsTypeDB()
    _ <- insertDepartmentsTypeDB()
    _ <- insertEmployeesTypeDB()
    _ <- insertJobsTypeDB()
    _ <- insertJobFunctionsTypeDB()
//    _ <- insertRelations()
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

  private def insertCitiesTypeDB() = migrationRepo.getHrCitiesStream
    .chunkN(1000)
    .parEvalMap(1) { chunk =>
      transactionManager.write(UUID.randomUUID(), Domain.HR).use { transaction =>
        for {
          _ <- Async[F]
                 .delay {
                   chunk
                     .map { c =>
                       val insertQueryStr =
                         s"""insert $$city isa city, has id "${c.id}", has region_id "${c.regionId}", has code "${c.code}", has name "${c.name}";"""
                       val insertQuery    = TypeQL.parseQuery[TypeQLInsert](insertQueryStr)
                       transaction.query().insert(insertQuery)
                     }
                 }
          _  = transaction.commit()
          _  = transaction.close()
          _ <- Logger[F].info(s"inserted ${chunk.size} cities")
        } yield ()
      }
    }
    .compile
    .drain

  private def insertLocationsTypeDB() = migrationRepo.getHrLocationsStream
    .chunkN(1000)
    .parEvalMap(1) { chunk =>
      transactionManager.write(UUID.randomUUID(), Domain.HR).use { transaction =>
        for {
          _ <- Async[F]
                 .delay {
                   chunk
                     .map { l =>
                       val insertQueryStr =
                         s"""insert $$location isa location, has id "${l.id}", has city_id "${l.cityId}", has code "${l.code}", has name "${l.name}";"""
                       val insertQuery    = TypeQL.parseQuery[TypeQLInsert](insertQueryStr)
                       transaction.query().insert(insertQuery)
                     }
                 }
          _  = transaction.commit()
          _  = transaction.close()
          _ <- Logger[F].info(s"inserted ${chunk.size} locations")
        } yield ()
      }
    }
    .compile
    .drain

  private def insertDepartmentsTypeDB() = migrationRepo.getHrDepartmentsStream
    .chunkN(1000)
    .parEvalMap(1) { chunk =>
      transactionManager.write(UUID.randomUUID(), Domain.HR).use { transaction =>
        for {
          _ <- Async[F]
                 .delay {
                   chunk
                     .map { d =>
                       val insertQueryStr =
                         s"""insert $$department isa department, has id "${d.id}", has location_id "${d.locationId}",
                            |has code "${d.code}", has name "${d.name}", has path "${d.path}";""".stripMargin
                       val insertQuery    = TypeQL.parseQuery[TypeQLInsert](insertQueryStr)
                       transaction.query().insert(insertQuery)
                     }
                 }
          _  = transaction.commit()
          _  = transaction.close()
          _ <- Logger[F].info(s"inserted ${chunk.size} departments")
        } yield ()
      }
    }
    .compile
    .drain

  private def insertEmployeesTypeDB() = migrationRepo.getHrEmployeesStream
    .take(1000) //be careful with typedb performance
    .chunkN(1000)
    .parEvalMap(8) { chunk =>
      transactionManager.write(UUID.randomUUID(), Domain.HR).use { transaction =>
        for {
          _ <- Async[F]
                 .delay {
                   chunk
                     .map { e =>
                       val insertQueryStr =
                         s"""insert $$employee isa employee, has id "${e.id}", has job_id "${e.jobId}",
                            |has department_id "${e.departmentId}", has gender ${e.gender}, has name "${e.name}", has email "${e.email}",
                            |has hired_date ${LocalDate
                           .ofInstant(e.hiredDate, ZoneId.systemDefault())
                           .toString}, has fired ${e.fired},
                            |has fired_date ${LocalDate
                           .ofInstant(e.firedDate.getOrElse(Instant.ofEpochMilli(0L)), ZoneId.systemDefault())},
                            |has path "${e.path}";""".stripMargin
//                       println(insertQueryStr)
                       val insertQuery    = TypeQL.parseQuery[TypeQLInsert](insertQueryStr)
                       transaction.query().insert(insertQuery)
                     }
                 }
          _  = transaction.commit()
          _  = transaction.close()
          _ <- Logger[F].info(s"inserted ${chunk.size} employees")
        } yield ()
      }
    }
    .compile
    .drain

  private def insertJobsTypeDB() = migrationRepo.getHrJobsStream
    .chunkN(1000)
    .parEvalMap(1) { chunk =>
      transactionManager.write(UUID.randomUUID(), Domain.HR).use { transaction =>
        for {
          _ <- Async[F]
                 .delay {
                   chunk
                     .map { j =>
                       val insertQueryStr =
                         s"""insert $$job isa job, has id "${j.id}", has function_id "${j.functionId}", has code "${j.code}", has name "${j.name}";"""
                       val insertQuery    = TypeQL.parseQuery[TypeQLInsert](insertQueryStr)
                       transaction.query().insert(insertQuery)
                     }
                 }
          _  = transaction.commit()
          _  = transaction.close()
          _ <- Logger[F].info(s"inserted ${chunk.size} jobs")
        } yield ()
      }
    }
    .compile
    .drain

  private def insertJobFunctionsTypeDB() = migrationRepo.getHrJobFunctionsStream
    .chunkN(1000)
    .parEvalMap(1) { chunk =>
      transactionManager.write(UUID.randomUUID(), Domain.HR).use { transaction =>
        for {
          _ <- Async[F]
                 .delay {
                   chunk
                     .map { f =>
                       val insertQueryStr =
                         s"""insert $$job_function isa job_function, has id "${f.id}", has code "${f.code}", has name "${f.name}";"""
                       val insertQuery    = TypeQL.parseQuery[TypeQLInsert](insertQueryStr)
                       transaction.query().insert(insertQuery)
                     }
                 }
          _  = transaction.commit()
          _  = transaction.close()
          _ <- Logger[F].info(s"inserted ${chunk.size} job_functions")
        } yield ()
      }
    }
    .compile
    .drain

  private def insertRelations() = {
    val regionCities        = "match $region isa region, has id $id;$city isa city ,has region_id = $id;" +
      "insert $region_cities(region_role: $region, city_role: $city) isa region_cities;"
    val cityLocations       = "match $city isa city, has id $id;$location isa location ,has city_id = $id;" +
      "insert $city_locations(city_role: $city, location_role: $location) isa city_locations;"
    val locationDepartments =
      "match $location isa location, has id $id;$department isa department ,has location_id = $id;" +
        "insert $location_departments(location_role: $location, department_role: $department) isa location_departments;"
    val departmentEmployees =
      "match $department isa department, has id $id;$employee isa employee ,has department_id = $id;" +
        "insert $department_employees(department_role: $department, employee_role: $employee) isa department_employees;"
    val jobEmployees        = "match $job isa job, has id $id;$employee isa employee ,has job_id = $id;" +
      "insert $job_employees(job_role: $job, employee_role: $employee) isa job_employees;"
    val functionJobs        = "match $job_function isa job_function, has id $id;$job isa job ,has function_id = $id;" +
      "insert $function_jobs(function_role:$job_function,job_role: $job) isa function_jobs;"
    Vector(regionCities, cityLocations, locationDepartments, departmentEmployees, jobEmployees, functionJobs).foldMapA {
      insertQueryStr =>
        transactionManager.write(UUID.randomUUID(), Domain.HR).use { transaction =>
          for {
            _ <- Async[F]
                   .blocking {
                     val insertQuery = TypeQL.parseQuery[TypeQLInsert](insertQueryStr)
                     transaction.query().insert(insertQuery)
                     transaction.commit()
                     transaction.close()
                   }
            _ <- Logger[F].info(s"inserted with $insertQueryStr")
          } yield ()
        }
    }
  }

}
