package text2ql.dao.typedb

import cats.effect._
import cats.implicits._
import com.vaticle.typedb.client.TypeDB
import com.vaticle.typedb.client.api._
import text2ql.api.Domain
import text2ql.configs.TypeDBConfig

import java.util.UUID

trait TypeDBTransactionManager[F[_]] {
  def read(requestId: UUID, domain: Domain): Resource[F, TypeDBTransaction]

  def write(requestId: UUID, domain: Domain): Resource[F, TypeDBTransaction]
}

object TypeDBTransactionManager {

  def apply[F[+_]: Sync](
      client: TypeDBClient,
      conf: TypeDBConfig
  ): Resource[F, TypeDBTransactionManager[F]] =
    Resource.eval(Sync[F].delay(new TypeDBTransactionManagerImpl(client, conf)))

}

final class TypeDBTransactionManagerImpl[F[+_]: Sync](
    client: TypeDBClient,
    conf: TypeDBConfig
) extends TypeDBTransactionManager[F] {

  override def read(requestId: UUID, domain: Domain): Resource[F, TypeDBTransaction] =
    openTypeDBSession(requestId, domain).flatMap { session =>
      val pureTransaction = Sync[F].delay(
        session.transaction(
          TypeDBTransaction.Type.READ,
          TypeDBOptions
            .core()
            .infer(conf.rules)
            .parallel(conf.parallel)
            .transactionTimeoutMillis(conf.transactionTimeoutMillis)
        )
      )

      Resource.fromAutoCloseable(pureTransaction)
    }

  override def write(requestId: UUID, domain: Domain): Resource[F, TypeDBTransaction] =
    openTypeDBSession(requestId, domain).flatMap { session =>
      val pureTransaction = Sync[F].delay(
        session.transaction(
          TypeDBTransaction.Type.WRITE,
          TypeDBOptions
            .core()
            .infer(conf.rules)
            .parallel(conf.parallel)
            .transactionTimeoutMillis(conf.transactionTimeoutMillis)
        )
      )

      Resource.fromAutoCloseable(pureTransaction)
    }

  private def openTypeDBSession(requestId: UUID, domain: Domain): Resource[F, TypeDBSession] =
    Resource.fromAutoCloseable(getDbName(domain).map(client.session(_, TypeDBSession.Type.DATA)))

  private def getDbName(domain: Domain): F[String] = Sync[F].delay {
    domain match {
      case Domain.HR => conf.keyspaceHr
    }
  }

}
