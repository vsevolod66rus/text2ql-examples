package text2ql.service

import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits._
import text2ql.api._
import text2ql.service.DomainSchemaService._

trait QueryDataUpdater[F[_]] {

  def updateDataForDBQuery(
      currentDataForQuery: DataForDBQuery,
      clarifiedEntities: List[ClarifiedNamedEntity],
      domain: Domain,
      slotName: String
  ): F[DataForDBQuery]

}

object QueryDataUpdater {

  def apply[F[+_]: Async](domainSchema: DomainSchemaService[F]): Resource[F, QueryDataUpdaterImpl[F]] =
    Resource.eval(Async[F].delay(new QueryDataUpdaterImpl(domainSchema)))
}

class QueryDataUpdaterImpl[F[+_]: Async](domainSchema: DomainSchemaService[F]) extends QueryDataUpdater[F] {

  override def updateDataForDBQuery(
      currentDataForQuery: DataForDBQuery,
      clarifiedEntities: List[ClarifiedNamedEntity],
      domain: Domain,
      slotName: String
  ): F[DataForDBQuery] =
    slotName match {
      case R_TYPE => currentDataForQuery.pure[F]
      case E_TYPE => updateDataForQueryWithEntity(currentDataForQuery, clarifiedEntities)
      case A_TYPE => updateDataForQueryWithAttributeType(currentDataForQuery, clarifiedEntities)
      case _      => updateDataForQueryWithAttribute(currentDataForQuery, clarifiedEntities, slotName)
    }

  private def updateDataForQueryWithEntity(
      currentDataForQuery: DataForDBQuery,
      clarifiedEntities: List[ClarifiedNamedEntity]
  ): F[DataForDBQuery] = for {
    entitiesFromSlot <- clarifiedEntities.view
                          .filter(_.tag == E_TYPE)
                          .map(e => ThingWithOriginalName(e.originalValue, e.getFirstNamedValue))
                          .toList
                          .pure[F]
    res              <- entitiesFromSlot.foldLeftM(currentDataForQuery)((acc, el) =>
                          chunkUpdateDataForQueryWithEntity(acc, clarifiedEntities, el)
                        )
  } yield res

  private def updateDataForQueryWithAttribute(
      currentDataForQuery: DataForDBQuery,
      clarifiedEntities: List[ClarifiedNamedEntity],
      slotName: String
  ): F[DataForDBQuery] = {
    val domain = currentDataForQuery.domain

    def collectAttributeValues(clarifiedEntity: ClarifiedNamedEntity): F[List[AttributeValue]] =
      domainSchema.schemaAttributesType(domain).map { attrs =>
        val comparisonOperator = getComparisonOperator(clarifiedEntities, clarifiedEntity)
        val joinValuesWithOr   = !attrs.get(slotName).contains("datetime")
        clarifiedEntity.namedValues.map(AttributeValue(_, comparisonOperator, joinValuesWithOr = joinValuesWithOr))
      }

    for {
      entities        <- clarifiedEntities.filter(_.tag == slotName).pure[F]
      attributeValues <- entities.traverse(collectAttributeValues).map(_.flatten)
      visual          <- domainSchema.visualAttributes(domain).map(_.contains(slotName))
      fullTextItems    = entities.flatMap(_.fullTextItems)
      attributes       = entities.map { e =>
                           val isTargetRoleRole = e.isTarget
                           AttributeForDBQuery(
                             attributeName = slotName,
                             attributeValues = attributeValues.some,
                             fullTextItems = fullTextItems,
                             includeGetClause = visual,
                             isTargetAttribute = isTargetRoleRole,
                             isAttributeFromRequest = true
                           )
                         }
      res             <- attributes.foldLeftM(currentDataForQuery) { (acc, el) =>
                           chunkUpdateDataForQueryWithAttributeType(acc, el)
                         }
    } yield res
  }

  private def updateDataForQueryWithAttributeType(
      currentDataForQuery: DataForDBQuery,
      clarifiedEntities: List[ClarifiedNamedEntity]
  ): F[DataForDBQuery] = {
    def collectAttributeForQuery(
        attributeTypeEntity: ClarifiedNamedEntity,
        visualAttributes: Seq[String]
    ): List[AttributeForDBQuery] =
      clarifiedEntities
        .filter(_.filterByTagAndValueOpt(A_TYPE, attributeTypeEntity.originalValue.some))
        .flatMap(_.namedValues)
        .map { name =>
          val ownEntities        = clarifiedEntities.view
            .filter(_.tag == "attribute_value")
            .filter(_.filterByGroupOpt(attributeTypeEntity.group))
          val isTargetRoleRole   = attributeTypeEntity.isTarget
          val comparisonOperator = getComparisonOperator(clarifiedEntities, attributeTypeEntity)
          val visual             = visualAttributes.contains(name)
          AttributeForDBQuery(
            attributeName = name,
            attributeValues = ownEntities.map(_.originalValue).toList.map(AttributeValue(_, comparisonOperator)).some,
            fullTextItems = ownEntities.toList.flatMap(_.fullTextItems),
            includeGetClause = visual,
            isTargetAttribute = isTargetRoleRole,
            isAttributeFromRequest = true
          )
        }

    for {
      domain                         <- currentDataForQuery.domain.pure[F]
      visualAttributes               <- domainSchema.visualAttributes(domain)
      attributesFromAttributeTypeSlot = clarifiedEntities
                                          .filter(_.tag == A_TYPE)
                                          .flatMap(collectAttributeForQuery(_, visualAttributes))
      res                            <- attributesFromAttributeTypeSlot.foldLeftM(currentDataForQuery)(chunkUpdateDataForQueryWithAttributeType)
    } yield res
  }

  private def chunkUpdateDataForQueryWithEntity(
      currentDataForQuery: DataForDBQuery,
      clarifiedEntities: List[ClarifiedNamedEntity],
      entity: ThingWithOriginalName
  ): F[DataForDBQuery] = for {
    entityName            <- entity.thingName.pure[F]
    domain                 = currentDataForQuery.domain
    keyAttr               <- domainSchema.thingKeys(domain).map(_.get(entityName))
    entityAttributesNames <- domainSchema.getAttributesByThing(domain)(entityName)
    isTargetRole           = clarifiedEntities
                               .filter(_.isTarget)
                               .exists(_.filterByTagAndValueOpt(E_TYPE, entity.originalName.some))
    currentEntityInDataOpt = currentDataForQuery.entityList.find(_.entityName == entityName)
    restEntities           = currentDataForQuery.entityList.filter(_.entityName != entityName)
    res                   <- isTargetRole
                               .pure[F]
                               .ifM(
                                 for {
                                   entityAttributes <-
                                     domainSchema.visualAttributes(domain).map { attrs =>
                                       entityAttributesNames
                                         .map { a =>
                                           AttributeForDBQuery(
                                             attributeName = a,
                                             includeGetClause = attrs.contains(a) || keyAttr.contains(a)
                                           )
                                         }
                                     }
                                   newEntity         = currentEntityInDataOpt.fold(
                                                         EntityForDBQuery(
                                                           entityName = entityName,
                                                           attributes = entityAttributes,
                                                           includeGetClause = true,
                                                           isTargetEntity = isTargetRole
                                                         )
                                                       )(e => e.copy(includeGetClause = true, isTargetEntity = isTargetRole))
                                   res              <-
                                     currentDataForQuery
                                       .copy(entityList = restEntities.filterNot(_.entityName == newEntity.entityName) :+ newEntity)
                                       .pure[F]
                                 } yield res,
                                 buildEmptyEntityAttrs(entityAttributesNames, entityName, domain)
                                   .map { attrs =>
                                     currentDataForQuery
                                       .copy(entityList =
                                         restEntities :+ currentEntityInDataOpt.fold(
                                           EntityForDBQuery(
                                             entityName = entityName,
                                             attributes = attrs,
                                             includeGetClause = true
                                           )
                                         )(e => e.copy(includeGetClause = true))
                                       )
                                   }
                               )
  } yield res

  private def chunkUpdateDataForQueryWithAttributeType(
      currentDataForQuery: DataForDBQuery,
      filterFromAttributeType: AttributeForDBQuery
  ): F[DataForDBQuery] = for {
    domain               <- currentDataForQuery.domain.pure[F]
    entityOrRelationName <- domainSchema.getThingByAttribute(domain)(filterFromAttributeType.attributeName)
    restAttributes       <- filterFromAttributeType.isTargetAttribute
                              .pure[F]
                              .ifM(
                                for {
                                  attrs <- domainSchema
                                             .getAttributesByThing(domain)(entityOrRelationName)
                                             .map(_.filter(_ != filterFromAttributeType.attributeName))
                                  res   <- buildEmptyEntityAttrs(attrs, entityOrRelationName, domain)
                                } yield res,
                                List.empty[AttributeForDBQuery].pure[F]
                              )
    entityName            = entityOrRelationName
    res                  <- for {
                              res <- currentDataForQuery.pure[F].map { data =>
                                       val entityAlreadyInDataOpt = data.entityList.find(_.entityName == entityName)
                                       val updatedEntity          = entityAlreadyInDataOpt
                                         .fold(
                                           EntityForDBQuery(
                                             entityName = entityName,
                                             attributes = filterFromAttributeType +: restAttributes,
                                             includeGetClause = true,
                                             isTargetEntity = filterFromAttributeType.isTargetAttribute
                                           )
                                         ) { e =>
                                           e.copy(
                                             attributes = e.attributes.filterNot(
                                               _.attributeName == filterFromAttributeType.attributeName
                                             ) :+ filterFromAttributeType,
                                             isTargetEntity = filterFromAttributeType.isTargetAttribute || e.isTargetEntity
                                           )
                                         }
                                       data.copy(entityList = data.entityList.filterNot {
                                         _.entityName == updatedEntity.entityName
                                       } :+ updatedEntity)
                                     }
                            } yield res
  } yield res

  private def getComparisonOperator(
      clarifiedEntities: List[ClarifiedNamedEntity],
      coEntity: ClarifiedNamedEntity
  ): String = {
    val comparisonOperatorOriginal = clarifiedEntities.view
      .filter(_.tag == CO)
      .find(_.filterByGroupOpt(coEntity.group))
      .map(_.originalValue)
    clarifiedEntities.view
      .filter(_.filterByGroupOpt(coEntity.group))
      .find(_.filterByTagAndValueOpt(CO, comparisonOperatorOriginal))
      .map(_.getFirstNamedValue)
      .getOrElse("=")
  }

  private def buildEmptyEntityAttrs(
      attrNames: List[String],
      entityName: String,
      domain: Domain
  ): F[List[AttributeForDBQuery]] = for {
    headlinesMap <- domainSchema.headlineAttributes(domain)
    keyAttrOpt   <- domainSchema.thingKeys(domain).map(_.get(entityName))
    headlineOpt   = headlinesMap.get(entityName)
    visual       <- domainSchema.visualAttributes(domain).map(_.toSet)
    res           = attrNames.map { a =>
                      AttributeForDBQuery(
                        attributeName = a,
                        includeGetClause = headlineOpt.contains(a) || keyAttrOpt.contains(a) || visual.contains(a)
                      )
                    }
  } yield res

}
