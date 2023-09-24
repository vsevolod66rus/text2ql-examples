import cats.Parallel
import cats.effect._
import cats.implicits._
import exceptions.CustomExceptionHandler
import org.typelevel.log4cats.Logger
import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax.CatsEffectConfigSource
import text2ql.configs.ApplicationConfig
import text2ql.controller.DomainSchemaController
import text2ql.service.{DomainSchemaChecker, DomainSchemaService}
import text2ql.{QueryBuilder, QueryManager, TransactorProvider, UnloggedTableManager, WSApp, WebService}

object App extends WSApp[ApplicationConfig] {
  private val changelog   = "migrations/Changelog.xml".some
  private val classLoader = getClass.getClassLoader

  override def service[F[+_]: Async: Parallel: Logger]: Resource[F, WebService[F]] = for {
    conf                <- Resource.eval(ConfigSource.default.loadF[F, ApplicationConfig]())
    xaStorage           <- TransactorProvider[F](changelog, classLoader, conf.database.storage)
    exceptionHandler    <- CustomExceptionHandler[F]
    domainSchema        <- DomainSchemaService[F]
    utm                 <- UnloggedTableManager[F](domainSchema, xaStorage)
    _                   <- QueryBuilder[F](domainSchema, utm, conf.database.data) //qb
    qm                  <- QueryManager[F](xaStorage)
    domainSchemaChecker <- DomainSchemaChecker[F](qm, utm, conf.database.data, conf.typeDB)
    controller          <- DomainSchemaController[F](domainSchema, domainSchemaChecker)
  } yield WebService[F](
    Seq(controller),
    exceptionHandler
  )
}
