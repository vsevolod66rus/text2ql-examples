import cats.Parallel
import cats.effect._
import cats.implicits._
import com.vaticle.typedb.client.TypeDB
import exceptions.CustomExceptionHandler
import org.typelevel.log4cats.Logger
import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax.CatsEffectConfigSource
import text2ql.configs.ApplicationConfig
import text2ql.controller.{DomainSchemaController, MigrationController}
import text2ql.repo.MigrationRepo
import text2ql.service.{DomainSchemaChecker, DomainSchemaService, MigrationService}
import text2ql.typedb.TypeDBTransactionManager
import text2ql.{QueryBuilder, QueryManager, TransactorProvider, UnloggedTableManager, WSApp, WebService}

object App extends WSApp[ApplicationConfig] {
  private val changelog   = "migrations/Changelog.xml".some
  private val classLoader = getClass.getClassLoader

  override def service[F[+_]: Async: Parallel: Logger]: Resource[F, WebService[F]] = for {
    conf                     <- Resource.eval(ConfigSource.default.loadF[F, ApplicationConfig]())
    xaStorage                <- TransactorProvider[F](changelog, classLoader, conf.database.storage)
    exceptionHandler         <- CustomExceptionHandler[F]
    domainSchema             <- DomainSchemaService[F]
    utm                      <- UnloggedTableManager[F](domainSchema, xaStorage)
    _                        <- QueryBuilder[F](domainSchema, utm, conf.database.data)
    qm                       <- QueryManager[F](xaStorage)
    typeDBClient             <- Resource.pure(TypeDB.coreClient(s"${conf.typeDB.url}"))
    typeDBTransactionManager <- TypeDBTransactionManager[F](typeDBClient, conf.typeDB)
    domainSchemaChecker      <- DomainSchemaChecker[F](qm, utm, conf.database.data, typeDBTransactionManager, conf.typeDB)
    migrationRepo            <- MigrationRepo[F](xaStorage)
    migrationService         <- MigrationService[F](migrationRepo, typeDBTransactionManager)
    schemaController         <- DomainSchemaController[F](domainSchema, domainSchemaChecker)
    migrationsController     <- MigrationController[F](migrationService)
  } yield WebService[F](
    Seq(schemaController, migrationsController),
    exceptionHandler
  )
}
