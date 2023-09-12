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
      domainSchema: DomainSchemaService[F],
      requestTypeCalculator: UserRequestTypeCalculator[F]
  ): Resource[F, QueryDataCalculatorImpl[F]] =
    Resource.eval(
      Async[F].delay(
        new QueryDataCalculatorImpl(
          updater,
          domainSchema,
          requestTypeCalculator
        )
      )
    )
}

class QueryDataCalculatorImpl[F[+_]: Async](
    updater: QueryDataUpdater[F],
    domainSchema: DomainSchemaService[F],
    requestTypeCalculator: UserRequestTypeCalculator[F]
) extends QueryDataCalculator[F] {

  override def prepareDataForQuery(
      clarifiedEntities: List[ClarifiedNamedEntity],
      userRequest: ChatMessageRequestModel,
      domain: Domain
  ): F[DataForDBQuery] =
    for {
      datetimeEntities        <- prepareDatetimeEntities(clarifiedEntities).pure[F]
      entities                 = {
        clarifiedEntities.filterNot { e =>
          dateAttributes.contains(e.tag) || e.tag == DATE_UNIT
        } ++ datetimeEntities
      }.flatMap(fixExistConfirmation)
      requestId                = userRequest.chat.requestId
      reqProperties           <- requestTypeCalculator.calculateDBQueryProperties(clarifiedEntities, domain)
      initialDataForQuery      = DataForDBQuery(
                                   requestId = requestId,
                                   entityList = List.empty[EntityForDBQuery],
                                   relationList = List.empty[RelationForDBQuery],
                                   logic = reqProperties,
                                   pagination = userRequest.some,
                                   domain = domain,
                                   requestType = reqProperties.requestType
                                 )
      nonEmptySlots            = entities
                                   .map(_.tag)
                                   .filterNot(slotsDoNotClarify.contains)
                                   .filterNot(functionEntities.contains)
                                   .distinct
                                   .sortWith { (s1, s2) =>
                                     val p1 = attributesPriorityForQueryDataCalculating.getOrElse(s1, Int.MaxValue)
                                     val p2 = attributesPriorityForQueryDataCalculating.getOrElse(s2, Int.MaxValue)
                                     p1 <= p2
                                   }
                                   .sortBy(s => !entities.filter(_.isTarget).map(_.tag).contains(s))
      dataForQuery            <-
        nonEmptySlots.foldLeftM(initialDataForQuery) { (acc, el) =>
          updater.updateDataForDBQuery(acc, entities, domain, el)
        }
      dataWithAttrsPriorities <- setAllAttributesPriority(dataForQuery)
      withVisualization        = calculateVisualization(dataWithAttrsPriorities)
    } yield withVisualization

  private def fixExistConfirmation(clarifiedEntity: ClarifiedNamedEntity): List[ClarifiedNamedEntity] = if (
    clarifiedEntity.tag == EXIST_CONFIRM
  )
    List(
      ClarifiedNamedEntity(
        tag = CO,
        originalValue = ">",
        start = clarifiedEntity.start + clarifiedEntity.originalValue.length + 1,
        namedValues = List(">"),
        attributeSelected = None,
        role = clarifiedEntity.role,
        group = clarifiedEntity.group
      ),
      ClarifiedNamedEntity(
        tag = "attribute_value",
        start = clarifiedEntity.start + clarifiedEntity.originalValue.length + 3,
        originalValue = "0",
        namedValues = List("0"),
        attributeSelected = None,
        role = clarifiedEntity.role,
        group = clarifiedEntity.group
      )
    )
  else List(clarifiedEntity)

  private def setAllAttributesPriority(data: DataForDBQuery): F[DataForDBQuery] = for {
    headlinesMap <- domainSchema.headlineAttributes(data.domain)
    headlines     = headlinesMap.values.toSet
    visual       <- domainSchema.visualAttributes(data.domain)
    entities     <- data.entityList
                      .traverse { e =>
                        e.attributes
                          .traverse { a =>
                            setThingAttributePriority(
                              isTargetThing = e.isTargetEntity,
                              isGeneralizingThing = e.isParent,
                              isHeadline = headlines.contains(a.attributeName),
                              isVisual = visual.contains(a.attributeName),
                              attribute = a,
                              domain = data.domain
                            )
                          }
                          .map(attrsWithPriority => e.copy(attributes = attrsWithPriority))
                      }
    relations    <- data.relationList
                      .traverse { r =>
                        r.attributes
                          .traverse { a =>
                            setThingAttributePriority(
                              isTargetThing = r.isTargetRelation,
                              isGeneralizingThing = false,
                              isHeadline = headlines.contains(a.attributeName),
                              isVisual = visual.contains(a.attributeName),
                              attribute = a,
                              domain = data.domain
                            )
                          }
                          .map(attrsWithPriority => r.copy(attributes = attrsWithPriority))
                      }
  } yield data.copy(relationList = relations, entityList = entities)

  private def setThingAttributePriority(
      isTargetThing: Boolean,
      isGeneralizingThing: Boolean,
      isHeadline: Boolean,
      isVisual: Boolean,
      attribute: AttributeForDBQuery,
      domain: Domain
  ): F[AttributeForDBQuery] = calculateAttributePriority(
    isHeadlineGeneralizingEntity = isHeadline && isGeneralizingThing,
    isHeadlineTargetEntity = isHeadline && isTargetThing,
    isTargetAttribute = attribute.isTargetAttribute,
    isAttributeFromRequest = attribute.isAttributeFromRequest,
    headline = isHeadline,
    visual = isVisual,
    attributeName = attribute.attributeName,
    domain = domain
  ).map(p => attribute.copy(attributePriority = p))

  private def calculateAttributePriority(
      isHeadlineGeneralizingEntity: Boolean,
      isHeadlineTargetEntity: Boolean,
      isTargetAttribute: Boolean,
      isAttributeFromRequest: Boolean,
      headline: Boolean,
      visual: Boolean,
      attributeName: String,
      domain: Domain
  ): F[AttributePriority] =
    (isHeadlineGeneralizingEntity, isHeadlineTargetEntity, isTargetAttribute, isAttributeFromRequest) match {
      case (true, _, _, _) =>
        domainSchema
          .visualAttributesSort(domain, attributeName)
          .map(sort => GeneralizingEntityHeadlinePriority(sort = sort))
      case (_, true, _, _) =>
        domainSchema
          .visualAttributesSort(domain, attributeName)
          .map(sort => TargetEntityHeadlinePriority(sort = sort))
      case (_, _, true, _) => Async[F].delay(TargetAttributePriority())
      case _ if headline   =>
        domainSchema.visualAttributesSort(domain, attributeName).map(sort => HeadlinePriority(sort = sort))
      case (_, _, _, true) => Async[F].delay(FromRequestPriority())
      case _ if visual     =>
        domainSchema.visualAttributesSort(domain, attributeName).map(sort => VisualPriority(sort = sort))
      case _               => Async[F].delay(DefaultPriority())
    }

  private def prepareDatetimeEntities(clarifiedEntities: List[ClarifiedNamedEntity]): List[ClarifiedNamedEntity] = {
    val dtTarget   = clarifiedEntities
      .find(_.tag == dtTARGET)
      .map(_.getFirstNamedValue)
      .getOrElse("empty datetime target")
    val dateGroups = clarifiedEntities.filter(_.tag == DATE).map(_.group).collect { case Some(g) => g }
    clarifiedEntities.collect {
      case e if e.isTarget && e.tag == DATE_UNIT                   =>
        e.copy(tag = e.attributeSelected.getOrElse("empty_attr_selected"), namedValues = List.empty)
      case e if e.tag == DATE                                      => e.copy(tag = dtTarget)
      case e if e.tag == CO && e.group.exists(dateGroups.contains) =>
        val coValue =
          if (Seq("from", ">", ">=").contains(e.getFirstNamedValue)) ">="
          else if (Seq("to", "<", "<=").contains(e.getFirstNamedValue)) "<="
          else "="
        e.copy(tag = CO, originalValue = coValue, namedValues = List(coValue))
    }
  }

  private def calculateVisualization(
      queryData: DataForDBQuery,
      nAttributesLimit: Int = 50,
      nPrimaryColumns: Int = 5
  ): DataForDBQuery = {
    val visualization = if (queryData.logic.unique) {
      val visualization =
        List(
          queryData.entityList
            .flatMap(_.attributes)
            .filter(_.includeGetClause),
          queryData.relationList
            .flatMap(_.attributes)
            .filter(_.includeGetClause)
        ).flatten

      val visualizationGeneralizingEntityHeadlinePriority =
        visualization
          .filter(a => AttributePriority.filterPriority[GeneralizingEntityHeadlinePriority](a.attributePriority))
          .sortBy(_.attributePriority match {
            case p: GeneralizingEntityHeadlinePriority => p.sort
            case _                                     => Int.MaxValue
          })

      val visualizationTargetEntityHeadlinePriority =
        visualization
          .filter(a => AttributePriority.filterPriority[TargetEntityHeadlinePriority](a.attributePriority))
          .sortBy(_.attributePriority match {
            case p: TargetEntityHeadlinePriority => p.sort
            case _                               => Int.MaxValue
          })

      val visualizationTargetAttributePriority =
        visualization.filter(a => AttributePriority.filterPriority[TargetAttributePriority](a.attributePriority))

      val visualizationHeadlinePriority =
        visualization
          .filter(a => AttributePriority.filterPriority[HeadlinePriority](a.attributePriority))
          .sortBy(_.attributePriority match {
            case p: HeadlinePriority => p.sort
            case _                   => Int.MaxValue
          })

      val visualizationFromRequestPriority =
        visualization.filter(a => AttributePriority.filterPriority[FromRequestPriority](a.attributePriority))

      val visualizationVisualPriority =
        visualization
          .filter(a => AttributePriority.filterPriority[VisualPriority](a.attributePriority))
          .sortBy(_.attributePriority match {
            case visualPriority: VisualPriority => visualPriority.sort
            case _                              => Int.MaxValue
          })

      val visualizationDefaultPriority =
        visualization.filter(a => AttributePriority.filterPriority[DefaultPriority](a.attributePriority))

      val primaryTagsList = List(
        visualizationGeneralizingEntityHeadlinePriority,
        visualizationTargetEntityHeadlinePriority,
        visualizationTargetAttributePriority,
        visualizationHeadlinePriority,
        visualizationFromRequestPriority
      ).flatten

      val visualizationList = List(
        primaryTagsList,
        visualizationVisualPriority,
        visualizationDefaultPriority
      ).flatten.map(_.attributeName)
      TagsVisualization(
        tags = visualizationList.distinct.take(nAttributesLimit),
        nPrimaryTags = primaryTagsList.size.max(nPrimaryColumns)
      )

    } else TagsVisualization()
    val updatedLogic  = queryData.logic.copy(visualization = visualization)
    queryData.copy(logic = updatedLogic)
  }
}
