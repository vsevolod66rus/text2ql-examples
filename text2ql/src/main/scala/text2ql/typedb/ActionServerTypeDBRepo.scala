package text2ql.typedb

import cats.effect.std.Semaphore
import cats.effect._
import cats.implicits._
import fs2.Stream
import org.typelevel.log4cats.Logger
import retry.RetryDetails._
import retry._
import text2ql.api.{AggregationLogic, AskRepoResponse, DataForDBQuery, Domain, GridPropertyFilterValue}
import text2ql.configs.TypeDBConfig
import text2ql.error.ServerError.{TypeDBConnectionsLimitExceeded, TypeDBQueryException}

import scala.concurrent.duration._

trait ActionServerTypeDBRepo[F[_]] {

  def generalTypeDBQueryWithPermitAndRetry(queryData: DataForDBQuery): F[AskRepoResponse]

  def getAttributeValuesWithPermitAndRetry(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      attrName: String,
      domain: Domain
  ): F[List[GridPropertyFilterValue]]

  def typeDbStreamQuery(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      domain: Domain
  ): F[Stream[F, Map[String, GridPropertyFilterValue]]]
}

object ActionServerTypeDBRepo {

  def apply[F[+_]: Async: Logger](
      queryManager: TypeDBQueryManager[F],
      transactionManager: TypeDBTransactionManager[F],
      conf: TypeDBConfig
  ): Resource[F, ActionServerTypeDBRepo[F]] =
    Resource.eval(
      Semaphore[F](conf.maxConcurrentTypeDB.toLong)
        .map(new ActionServerTypeDBRepoImpl(_, queryManager, transactionManager, conf))
    )
}

class ActionServerTypeDBRepoImpl[F[+_]: Async: Logger](
    semaphore: Semaphore[F],
    queryManager: TypeDBQueryManager[F],
    transactionManager: TypeDBTransactionManager[F],
    conf: TypeDBConfig
) extends ActionServerTypeDBRepo[F] {

  def generalTypeDBQueryWithPermitAndRetry(queryData: DataForDBQuery): F[AskRepoResponse] = retryOnSomeErrors(
    semaphore.tryAcquire.ifM(
      semaphore.count.flatMap(c => Logger[F].info(s"typeDB semaphore count = $c, query = general")) >>
        Async[F].guarantee(
          queryManager
            .generalQuery(queryData, queryData.logic, queryData.domain)
            .adaptError(TypeDBQueryException(_)),
          semaphore.release
        ),
      Async[F].raiseError(TypeDBConnectionsLimitExceeded)
    )
  )

  def getAttributeValuesWithPermitAndRetry(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      attrName: String,
      domain: Domain
  ): F[List[GridPropertyFilterValue]] = retryOnSomeErrors(
    semaphore.tryAcquire.ifM(
      semaphore.count.flatMap(c => Logger[F].info(s"typeDB semaphore count = $c, query = general")) >>
        Async[F].guarantee(
          queryManager.getAttributeValues(queryData, logic, attrName, domain),
          semaphore.release
        ),
      Async[F].raiseError(TypeDBConnectionsLimitExceeded)
    )
  )

  def typeDbStreamQuery(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      domain: Domain
  ): F[Stream[F, Map[String, GridPropertyFilterValue]]] = Async[F].delay {
    for {
      readTransaction <- Stream.resource(transactionManager.read(queryData.requestId, domain))
      answer          <- Stream
                           .eval(
                             queryManager.streamQuery(
                               queryData,
                               logic,
                               readTransaction,
                               domain
                             )
                           )
                           .flatten
    } yield answer
  }

  private def retryOnSomeErrors[A](effect: F[A]): F[A] = retryingOnSomeErrors(
    policy = retryPolice(conf.limitRetries, conf.constantDelay),
    isWorthRetrying = checkRetryError,
    onError = logError
  )(effect)

  private def retryPolice(limitRetries: Int, constantDelay: Int): RetryPolicy[F] =
    RetryPolicies.limitRetries[F](limitRetries).join(RetryPolicies.constantDelay(constantDelay.seconds))

  private def checkRetryError(err: Throwable): F[Boolean] = Async[F].pure {
    err match {
      case TypeDBConnectionsLimitExceeded                                => true
      case e if e.toString.contains("Invalid Session Operation")         => true
      case e if e.getMessage.contains("The session has been closed")     => true
      case e if e.getMessage.contains("The transaction has been closed") => true
      case _                                                             => false
    }
  }

  private def logError(err: Throwable, details: RetryDetails): F[Unit] =
    err match {
      case TypeDBConnectionsLimitExceeded =>
        details match {
          case WillDelayAndRetry(_, retriesSoFar: Int, _)              =>
            Logger[F].info(s"Ожидание свободного подключения к typedDB. Использовано $retriesSoFar попыток")
          case GivingUp(totalRetries: Int, totalDelay: FiniteDuration) =>
            Logger[F].info(
              s"Ожидание свободного подключения к typeDB - безуспешно после $totalRetries попыток, всего ожидания ${totalDelay.toString}."
            )
        }
      case _                              => Logger[F].error(s"Ошибка TypeDB: ${err.getMessage}")
    }

}
