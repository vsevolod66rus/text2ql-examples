package text2ql.service

import cats.effect.{Resource, Sync}
import cats.implicits._
import text2ql.api.UserRequestType.{statsRT, withGroupingRT}
import text2ql.api._
import text2ql.error.ServerError.ServerErrorWithMessage
import text2ql.service.DomainSchemaService._

trait UserRequestTypeCalculator[F[_]] {
  def calculateDBQueryProperties(entities: List[ClarifiedNamedEntity], domain: Domain): F[AggregationLogic]
}

object UserRequestTypeCalculator {

  def apply[F[+_]: Sync](domainSchema: DomainSchemaService[F]): Resource[F, UserRequestTypeCalculator[F]] =
    Resource.eval(Sync[F].delay(new UserRequestTypeCalculatorImpl[F](domainSchema)))
}

class UserRequestTypeCalculatorImpl[F[+_]: Sync](domainSchema: DomainSchemaService[F])
    extends UserRequestTypeCalculator[F] {

  private def isNumericAttr(attributeTypesMap: Map[String, String], nerOpt: Option[ClarifiedNamedEntity]): Boolean =
    nerOpt.exists { e =>
      attributeTypesMap.get(e.getFirstNamedValue).exists(Set("double", "long").contains)
    }

  def calculateDBQueryProperties(entities: List[ClarifiedNamedEntity], domain: Domain): F[AggregationLogic] = for {
    attributeTypesMap <- domainSchema.schemaAttributesType(domain)
    extremumOpt        = entities.find(_.tag == EXTREMUM)
    statsOpt           = entities.find(_.tag == STATS)
    chartOpt           = entities.find(_.tag == CHART)
    argumentOpt        = entities.find(_.role.contains(ARGUMENT))
    targetOpt          = entities.find(_.isTarget)
    isNumericArgument  = isNumericAttr(attributeTypesMap, argumentOpt)
    isNumericTarget    = isNumericAttr(attributeTypesMap, targetOpt)
    res               <- (extremumOpt, statsOpt, chartOpt, argumentOpt, targetOpt) match {
                           case (None, None, Some(_), Some(_), Some(_))                      =>
                             buildGetInstanceListProperties(
                               entities,
                               domain,
                               targetOpt,
                               argumentOpt,
                               UserRequestType.CountInstancesInGroups
                             )
                           case (None, Some(_), None, Some(_), Some(_)) if isNumericArgument =>
                             buildGetInstanceListProperties(
                               entities,
                               domain,
                               targetOpt,
                               argumentOpt,
                               UserRequestType.GetNumericValueDescription
                             )
                           case (None, Some(_), None, Some(_), Some(_))                      =>
                             buildGetInstanceListProperties(
                               entities,
                               domain,
                               targetOpt,
                               argumentOpt,
                               UserRequestType.GetStringValueDescription
                             )
                           case (_, Some(_), Some(_), Some(_), Some(_)) if isNumericTarget   =>
                             buildGetInstanceListProperties(
                               entities,
                               domain,
                               targetOpt,
                               argumentOpt,
                               UserRequestType.SumAttributeValuesInGroups
                             )
                           case (Some(_), None, None, Some(_), Some(_))                      =>
                             buildGetInstanceListProperties(
                               entities,
                               domain,
                               targetOpt,
                               None,
                               UserRequestType.GetInstanceWithMaxAttribute
                             )
                           case (Some(_), None, Some(_), Some(_), Some(_))                   =>
                             buildGetInstanceListProperties(
                               entities,
                               domain,
                               targetOpt,
                               argumentOpt,
                               UserRequestType.MaxCountInGroups
                             )
                           case _                                                            =>
                             buildGetInstanceListProperties(
                               entities,
                               domain,
                               targetOpt,
                               None,
                               UserRequestType.GetInstanceList
                             )
                         }
  } yield res

  private def buildGetInstanceListProperties(
      entities: List[ClarifiedNamedEntity],
      domain: Domain,
      targetOpt: Option[ClarifiedNamedEntity],
      groupByOpt: Option[ClarifiedNamedEntity],
      userRequestType: UserRequestType
  ): F[AggregationLogic] = {
    val unique  = !withGroupingRT.contains(userRequestType)
    val isStats = statsRT.contains(userRequestType)
    for {
      _                 <- "".pure[F]
      targetVertexName  <- getTargetVertexName(targetOpt, domain)
      targetAttrName    <- getTargetAttrName(targetOpt, domain, isGroupBy = false)
      groupByVertexName <- if (unique && !isStats) "".pure[F] else getTargetVertexName(groupByOpt, domain)
      groupByAttrName   <- if (unique && !isStats) "".pure[F] else getTargetAttrName(groupByOpt, domain, isGroupBy = true)
      subAttrOpt         = (if (unique) None else groupByOpt.flatMap(_.findFirstNamedValue)).filter(dateAttributes.contains)
    } yield AggregationLogic(
      unique = unique,
      targetAttr = targetAttrName,
      targetThing = targetVertexName,
      groupByAttr = groupByAttrName,
      groupByThing = groupByVertexName,
      subAttrOpt = subAttrOpt,
      aggregationFilterOpt = findAggregationFilter(entities),
      sortModelOpt = findSortModel(userRequestType, entities),
      requestType = userRequestType
    )
  }

  private def getTargetVertexName(targetOpt: Option[ClarifiedNamedEntity], domain: Domain): F[String] = for {
    targetEntity        <- Sync[F].fromOption(targetOpt, ServerErrorWithMessage("no target entity from nlp"))
    targetEntityNameOpt <- targetEntity.tag match {
                             case E_TYPE    => targetEntity.findFirstNamedValue.pure[F]
                             case A_TYPE    =>
                               targetEntity.findFirstNamedValue.traverse(v =>
                                 domainSchema.getThingByAttribute(domain)(v)
                               )
                             case DATE_UNIT =>
                               targetEntity.attributeSelected.traverse(v => domainSchema.getThingByAttribute(domain)(v))
                             case _         => domainSchema.getThingByAttribute(domain)(targetEntity.tag).map(_.some)
                           }
    targetEntityName    <-
      Sync[F].fromOption(targetEntityNameOpt, ServerErrorWithMessage("no selected value for target entity"))
  } yield targetEntityName

  private def getTargetAttrName(
      targetOpt: Option[ClarifiedNamedEntity],
      domain: Domain,
      isGroupBy: Boolean
  ): F[String] = for {
    targetEntity        <- Sync[F].fromOption(targetOpt, ServerErrorWithMessage("no target entity from nlp"))
    targetEntityNameOpt <- targetEntity.tag match {
                             case E_TYPE    =>
                               val namedValue = targetEntity.findFirstNamedValue
                               if (isGroupBy)
                                 namedValue.traverse { v =>
                                   domainSchema.headlineAttributes(domain).map(_.getOrElse(v, v))
                                 }
                               else namedValue.traverse(v => domainSchema.thingKeys(domain).map(_.getOrElse(v, v)))
                             case DATE_UNIT => targetEntity.attributeSelected.pure[F]
                             case _         => targetEntity.findFirstNamedValue.pure[F]
                           }
    targetAttrName      <-
      Sync[F].fromOption(targetEntityNameOpt, ServerErrorWithMessage("no selected value for target attr"))
  } yield targetAttrName

  private def findAggregationFilter(clarifiedEntities: List[ClarifiedNamedEntity]): Option[AggregationFilter] =
    clarifiedEntities
      .filter(_.tag == E_TYPE)
      .filter(_.filterByRole(ARGUMENT))
      .map { e =>
        val co        = clarifiedEntities
          .filter(_.tag == CO)
          .find(_.filterByGroupOpt(e.group))
          .map(_.getFirstNamedValue)
        val attrValue = clarifiedEntities
          .filter(_.tag == "attribute_value")
          .find(_.filterByGroupOpt(e.group))
          .map(_.getFirstNamedValue)
        (co, attrValue).tupled.map { case (c, v) =>
          AggregationFilter(comparisonOperator = c, value = v)
        }
      }
      .collectFirst { case Some(f) => f }
      .filter(_ => clarifiedEntities.exists(_.filterByRole(AGGREGATION)))

  private def findSortModel(
      userRequestType: UserRequestType,
      clarifiedEntities: List[ClarifiedNamedEntity]
  ): Option[BaseSortModel] = {
    val directionOpt = clarifiedEntities
      .find(_.tag == EXTREMUM)
      .filter(_.namedValues.contains("min"))
      .fold[SortDirection](SortDirection.desc)(_ => SortDirection.asc)
      .some
    userRequestType match {
      case UserRequestType.GetInstanceWithMaxAttribute =>
        val orderByOpt = clarifiedEntities
          .filterNot(e => Set(E_TYPE, R_TYPE, EXTREMUM, STATS, DATE_UNIT).contains(e.tag))
          .find(_.role.exists(_ == ARGUMENT))
          .map(_.getFirstNamedValue)
        (orderByOpt, directionOpt).tupled.collect { case (orderBy, direction) =>
          BaseSortModel(orderBy.some, direction.some)
        }
      case UserRequestType.MaxCountInGroups            => BaseSortModel("counting".some, directionOpt).some
      case _                                           => None
    }
  }

}
