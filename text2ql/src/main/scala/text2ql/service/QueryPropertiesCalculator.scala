package text2ql.service

import cats.effect.{Resource, Sync}
import cats.implicits._
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

  def calculateDBQueryProperties(entities: List[ClarifiedNamedEntity], domain: Domain): F[AggregationLogic] = for {
    attributeTypesMap <- domainSchema.schemaAttributesType(domain)
    extremumOpt        = entities.find(_.tag == EXTREMUM)
    statsOpt           = entities.find(_.tag == STATS)
    chartOpt           = entities.find(_.tag == CHART)
    argumentOpt        = entities.find(_.role.contains(ARGUMENT))
    targetOpt          = entities.find(_.isTarget)
    res               <- (extremumOpt, statsOpt, chartOpt, argumentOpt, targetOpt) match {
                           case (None, None, Some(_), Some(_), Some(_)) =>
                             buildGetInstanceListProperties(
                               entities,
                               domain,
                               targetOpt,
                               argumentOpt,
                               UserRequestType.CountInstancesInGroups
                             )
                           case _                                       =>
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
  ): F[AggregationLogic] =
    for {
      unique            <- entities.exists(_.tag == CHART).pure[F].map(!_)
      targetVertexName  <- getTargetVertexName(targetOpt, domain)
      targetAttrName    <- getTargetAttrName(targetOpt, domain, isGroupBy = false)
      groupByVertexName <- if (unique && groupByOpt.isEmpty) "".pure[F] else getTargetVertexName(groupByOpt, domain)
      groupByAttrName   <-
        if (unique && groupByOpt.isEmpty) "".pure[F] else getTargetAttrName(groupByOpt, domain, isGroupBy = true)
    } yield AggregationLogic(
      unique = unique,
      targetAttr = targetAttrName,
      targetThing = targetVertexName,
      groupByAttr = groupByAttrName,
      groupByThing = groupByVertexName,
      sortModelOpt = None
    )

  private def getTargetVertexName(targetOpt: Option[ClarifiedNamedEntity], domain: Domain): F[String] = for {
    targetEntity        <- Sync[F].fromOption(targetOpt, ServerErrorWithMessage("no target entity from nlp"))
    targetEntityNameOpt <- targetEntity.tag match {
                             case E_TYPE    => targetEntity.findFirstNamedValue.pure[F]
                             case A_TYPE    =>
                               targetEntity.findFirstNamedValue.traverse(v =>
                                 domainSchema.getThingByAttribute(domain)(v)
                               )
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
                             case _         => targetEntity.findFirstNamedValue.pure[F]
                           }
    targetAttrName      <-
      Sync[F].fromOption(targetEntityNameOpt, ServerErrorWithMessage("no selected value for target attr"))
  } yield targetAttrName

}
