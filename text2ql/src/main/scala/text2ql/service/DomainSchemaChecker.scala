package text2ql.service

import cats.effect.{Async, Resource}
import cats.implicits._
import fs2.{Stream, text}
import org.typelevel.log4cats.Logger
import text2ql.{QueryBuilder, QueryManager, UnloggedTableManager}
import text2ql.api._
import text2ql.configs.{DBDataConfig, TypeDBConfig}
import text2ql.domainschema.CheckDomainSchemaResponse
import text2ql.repo.{AskRepo, AskResponseBuilder}
import text2ql.service.DomainSchemaService._
import text2ql.typedb._

import java.util.UUID

trait DomainSchemaChecker[F[_]] {
  def baseCheck(domain: Domain, content: Stream[F, Byte]): F[List[CheckDomainSchemaResponse]]

  def relationsCheck(
      domain: Domain,
      entities: List[String],
      onlySet: Boolean,
      content: Stream[F, Byte]
  ): F[List[CheckDomainSchemaResponse]]

  def baseCheckTypeDB(domain: Domain, content: Stream[F, Byte]): F[List[CheckDomainSchemaResponse]]

  def relationsCheckTypeDB(
      domain: Domain,
      entities: List[String],
      onlySet: Boolean,
      content: Stream[F, Byte]
  ): F[List[CheckDomainSchemaResponse]]
}

object DomainSchemaChecker {

  def apply[F[+_]: Async: Logger](
      qm: QueryManager[F],
      utm: UnloggedTableManager[F],
      conf: DBDataConfig,
      typeDBTransactionManager: TypeDBTransactionManager[F],
      typeDBConf: TypeDBConfig
  ): Resource[F, DomainSchemaChecker[F]] = for {
    domainSchemaServ      <- DomainSchemaService[F]
    updater               <- QueryDataUpdater[F](domainSchemaServ)
    requestTypeCalculator <- UserRequestTypeCalculator[F](domainSchemaServ)
    queryDataCalc         <-
      QueryDataCalculator[F](updater, domainSchemaServ, requestTypeCalculator)
    qb                    <- QueryBuilder[F](domainSchemaServ, utm, conf)
    rb                    <- AskResponseBuilder[F](domainSchemaServ)
    repo                  <- AskRepo[F](qb, qm, utm, rb)

    typeDBQueryHelper  <- TypeDBQueryHelper[F](domainSchemaServ)
    typeDBQueryBuilder <- TypeDBQueryBuilder[F](typeDBQueryHelper, domainSchemaServ)
    typeDBQueryManager <-
      TypeDBQueryManager[F](typeDBTransactionManager, typeDBQueryBuilder, typeDBQueryHelper, typeDBConf)

    typeDBRepo <- ActionServerTypeDBRepo[F](typeDBQueryManager, typeDBTransactionManager, typeDBConf)

  } yield new DomainSchemaCheckerImpl[F](queryDataCalc, repo, typeDBRepo, domainSchemaServ, qb)
}

class DomainSchemaCheckerImpl[F[+_]: Async](
    queryDataCalculator: QueryDataCalculator[F],
    askRepo: AskRepo[F],
    askRepoTypeDB: ActionServerTypeDBRepo[F],
    domainSchema: DomainSchemaService[F],
    qb: QueryBuilder[F]
) extends DomainSchemaChecker[F] {

  def baseCheck(domain: Domain, content: Stream[F, Byte]): F[List[CheckDomainSchemaResponse]] = for {
    _             <- content.through(text.utf8.decode).compile.string.flatMap(domainSchema.update(domain, _))
    entityNames   <- domainSchema.vertices(domain).map(_.map(_.vertexName).toList)
    samples        = buildClarifiedEntities(entityNames.map(List(_)))
    queryDataList <- samples.traverse { e =>
                       queryDataCalculator.prepareDataForQuery(e, baseUserReq, domain)
                     }
    res           <- askQueries(queryDataList)
  } yield res

  def relationsCheck(
      domain: Domain,
      entities: List[String],
      onlySet: Boolean,
      content: Stream[F, Byte]
  ): F[List[CheckDomainSchemaResponse]] = for {
    _                   <- content.through(text.utf8.decode).compile.string.flatMap(domainSchema.update(domain, _))
    entityNames         <- domainSchema.vertices(domain).map(_.map(_.vertexName).toList).map(_.filter(entities.contains))
    entitiesSeq          = if (onlySet) List(entityNames) else (1 to entityNames.size).flatMap(entityNames.combinations).toList
    samples              = buildClarifiedEntities(entitiesSeq)
    queryDataListEither <- samples.traverse { entities =>
                             queryDataCalculator.prepareDataForQuery(entities, baseUserReq, domain).attempt
                           }
    queryDataList        = queryDataListEither.collect { case Right(value) => value }
    res                 <- askQueries(queryDataList)
  } yield res

  def baseCheckTypeDB(domain: Domain, content: Stream[F, Byte]): F[List[CheckDomainSchemaResponse]] = for {
    _             <- content.through(text.utf8.decode).compile.string.flatMap(domainSchema.update(domain, _))
    entityNames   <- domainSchema.vertices(domain).map(_.map(_.vertexName).toList)
    samples        = buildClarifiedEntities(entityNames.map(List(_)))
    queryDataList <- samples.traverse { e =>
                       queryDataCalculator.prepareDataForQuery(e, baseUserReq, domain)
                     }
    res           <- askQueriesTypeDB(queryDataList)
  } yield res

  def relationsCheckTypeDB(
      domain: Domain,
      entities: List[String],
      onlySet: Boolean,
      content: Stream[F, Byte]
  ): F[List[CheckDomainSchemaResponse]] = for {
    _                   <- content.through(text.utf8.decode).compile.string.flatMap(domainSchema.update(domain, _))
    entityNames         <- domainSchema.vertices(domain).map(_.map(_.vertexName).toList).map(_.filter(entities.contains))
    entitiesSeq          = if (onlySet) List(entityNames) else (1 to entityNames.size).flatMap(entityNames.combinations).toList
    samples              = buildClarifiedEntities(entitiesSeq)
    queryDataListEither <- samples.traverse { entities =>
                             queryDataCalculator.prepareDataForQuery(entities, baseUserReq, domain).attempt
                           }
    queryDataList        = queryDataListEither.collect { case Right(value) => value }
    res                 <- askQueriesTypeDB(queryDataList)
  } yield res

  private val baseUserReq = ChatMessageRequestModel(
    page = 0.some,
    perPage = 10.some,
    sort = BaseSortModel(None, None),
    chat = AddChatMessageRequestModel(message = "", requestId = UUID.randomUUID())
  )

  private def buildClarifiedEntities(entitiesList: List[List[String]]): List[List[ClarifiedNamedEntity]] =
    entitiesList.map { names =>
      names.map { e =>
        ClarifiedNamedEntity(
          tag = E_TYPE,
          originalValue = e,
          start = 0,
          namedValues = List(e),
          attributeSelected = None,
          isTarget = names.headOption.contains(e)
        )
      }
    }

  private def askQueries(queryDataList: List[DataForDBQuery]): F[List[CheckDomainSchemaResponse]] =
    queryDataList.traverse { queryData =>
      for {
        constantSqlChunk <- qb.buildConstantSqlChunk(queryData)
        buildQueryDTO    <- qb.buildGeneralSqlQuery(queryData, constantSqlChunk)
        res              <- askRepo
                              .generalQuery(queryData)
                              .attempt
                              .map(_.map(_.custom.flatMap(_.grid).get).leftMap(_.getMessage))
        entities          = queryData.entityList.map(_.entityName)
        relations         = queryData.relationList.map(_.relationName)
      } yield CheckDomainSchemaResponse(entities, relations, buildQueryDTO, res)
    }

  private def askQueriesTypeDB(queryDataList: List[DataForDBQuery]): F[List[CheckDomainSchemaResponse]] =
    queryDataList.traverse { queryData =>
      for {
        constantSqlChunk <- qb.buildConstantSqlChunk(queryData)
        buildQueryDTO    <- qb.buildGeneralSqlQuery(queryData, constantSqlChunk)
        res              <-
          askRepoTypeDB
            .generalTypeDBQueryWithPermitAndRetry(queryData)
            .attempt
            .map(_.map(_.custom.flatMap(_.grid).getOrElse(GridWithDataRenderTypeResponseModel())).leftMap(_.toString))
        entities          = queryData.entityList.map(_.entityName)
        relations         = queryData.relationList.map(_.relationName)
      } yield CheckDomainSchemaResponse(entities, relations, buildQueryDTO, res)
    }
}
