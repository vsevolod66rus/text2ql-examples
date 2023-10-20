package text2ql.service

import cats.effect.{Async, Resource}
import cats.implicits._
import text2ql.api._
import text2ql.service.DomainSchemaService._

trait QueryDataCalculator[F[_]] {

  def prepareDataForQuery(
      clarifiedEntities: List[ClarifiedNamedEntity],
      userRequest: ChatMessageRequestModel,
      domain: Domain
  ): F[DataForDBQuery]
}

object QueryDataCalculator {

  def apply[F[+_]: Async](
      updater: QueryDataUpdater[F],
      requestTypeCalculator: UserRequestTypeCalculator[F]
  ): Resource[F, QueryDataCalculatorImpl[F]] =
    Resource.eval(Async[F].delay(new QueryDataCalculatorImpl(updater, requestTypeCalculator)))
}

class QueryDataCalculatorImpl[F[+_]: Async](
    updater: QueryDataUpdater[F],
    requestTypeCalculator: UserRequestTypeCalculator[F]
) extends QueryDataCalculator[F] {

  override def prepareDataForQuery(
      clarifiedEntities: List[ClarifiedNamedEntity],
      userRequest: ChatMessageRequestModel,
      domain: Domain
  ): F[DataForDBQuery] =
    for {
      reqProperties      <- requestTypeCalculator.calculateDBQueryProperties(clarifiedEntities, domain)
      requestId           = userRequest.chat.requestId
      initialDataForQuery = DataForDBQuery(
                              requestId = requestId,
                              entityList = List.empty[EntityForDBQuery],
                              relationList = List.empty[RelationForDBQuery],
                              logic = reqProperties,
                              pagination = userRequest.some,
                              domain = domain
                            )
      nonEmptySlots       = clarifiedEntities
                              .map(_.tag)
                              .filterNot(slotsDoNotClarify.contains)
                              .filterNot(functionEntities.contains)
                              .distinct
                              .sortWith { (s1, s2) =>
                                val p1 = attributesPriorityForQueryDataCalculating.getOrElse(s1, Int.MaxValue)
                                val p2 = attributesPriorityForQueryDataCalculating.getOrElse(s2, Int.MaxValue)
                                p1 <= p2
                              }
                              .sortBy(s => !clarifiedEntities.filter(_.isTarget).map(_.tag).contains(s))
      dataForQuery       <-
        nonEmptySlots.foldLeftM(initialDataForQuery) { (acc, el) =>
          updater.updateDataForDBQuery(acc, clarifiedEntities, domain, el)
        }
      withVisualization   = calculateVisualization(dataForQuery)
    } yield withVisualization

  private def calculateVisualization(
      queryData: DataForDBQuery,
      nAttributesLimit: Int = 50
  ): DataForDBQuery = {
    val visualization = if (queryData.logic.unique) {
      val visualization =
        List(
          queryData.entityList.flatMap(_.attributes),
          queryData.relationList.flatMap(_.attributes)
        ).flatten.map(_.attributeName)
      TagsVisualization(tags = visualization.take(nAttributesLimit))
    } else TagsVisualization()
    val updatedLogic  = queryData.logic.copy(visualization = visualization)
    queryData.copy(logic = updatedLogic)
  }
}
