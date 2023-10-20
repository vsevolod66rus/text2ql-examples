package text2ql.dao.typedb

import cats.effect.kernel.{Resource, Sync}
import cats.implicits._
import com.vaticle.typedb.client.api.answer.ConceptMap
import text2ql.api._
import text2ql.service.DomainSchemaService
import text2ql.service.DomainSchemaService._

import java.time.LocalDateTime
import scala.util.Try

trait TypeDBResponseBuilder[F[_]] {

  def collectAggregateClause(attributes: Seq[AttributeForDBQuery], domain: Domain): F[String]

  def makeGridProperties(
      queryData: DataForDBQuery,
      raw: Seq[ConceptMap],
      logic: AggregationLogic,
      domain: Domain,
      isSortable: String => Boolean
  ): F[(List[GridPropertyItemModel], List[ExtractedDataForAggregation])]

  def makeGrid(
      queryData: DataForDBQuery,
      raw: Seq[ConceptMap],
      logic: AggregationLogic,
      total: Long,
      domain: Domain,
      offset: Int,
      limit: Int
  ): F[Option[GridWithDataRenderTypeResponseModel]]

  def getCMAttributeValue(
      cm: ConceptMap,
      attribute: String,
      domain: Domain
  ): F[GridPropertyFilterValue]

  def getCMAttributeStringValue(
      cm: ConceptMap,
      attribute: String
  ): String
}

object TypeDBResponseBuilder {

  def apply[F[_]: Sync](
      domainSchema: DomainSchemaService[F]
  ): Resource[F, TypeDBResponseBuilder[F]] =
    Resource.eval(Sync[F].delay(new TypeDBResponseBuilderImpl(domainSchema)))
}

class TypeDBResponseBuilderImpl[F[_]: Sync](domainSchema: DomainSchemaService[F]) extends TypeDBResponseBuilder[F] {

  override def collectAggregateClause(attributes: Seq[AttributeForDBQuery], domain: Domain): F[String] =
    attributes
      .filter(_.attributeValues.nonEmpty)
      .traverse { attribute =>
        for {
          attributeType <-
            domainSchema.schemaAttributesType(domain).map(_.getOrElse(attribute.attributeName, "string"))
          joinValuesPart = if (attribute.attributeValues.exists(_.joinValuesWithOr)) "or" else ";"
          clause         = attributeType match {
                             case "string" =>
                               val co = "="
                               attribute.attributeValues
                                 .map(el => s"{$$${attribute.attributeName} $co '${el.value}';}")
                                 .mkString("", s" $joinValuesPart ", ";")
                             case _        =>
                               attribute.attributeValues
                                 .map(a => if (attributeType == "boolean") a.copy(value = a.value.toLowerCase) else a)
                                 .map(el => s"{$$${attribute.attributeName} ${el.comparisonOperator} ${el.value};}")
                                 .mkString("", s" $joinValuesPart ", ";")

                           }
        } yield clause
      }
      .map(_.mkString)

  override def makeGridProperties(
      queryData: DataForDBQuery,
      raw: Seq[ConceptMap],
      logic: AggregationLogic,
      domain: Domain,
      isSortable: String => Boolean
  ): F[(List[GridPropertyItemModel], List[ExtractedDataForAggregation])] = if (logic.unique)
    logic.visualization.tags
      .traverse { attribute =>
        for {
          attrType <- domainSchema.schemaAttributesType(domain).map(_.getOrElse(attribute, "string"))
          title    <- domainSchema.attributesTitle(domain).map(_.getOrElse(attribute, attribute))
        } yield GridPropertyItemModel(
          key = attribute,
          title = title,
          dataType = GridPropertyDataType.fromType(attrType),
          filter = GridPropertyFilterModel.fromAttrType(attrType, isCategorical = true)
        )
      }
      .map(_ -> List.empty)
  else
    for {
      aggregationOriginalName <- Sync[F].delay(logic.groupByAttr)
      countingOriginalName    <- Sync[F].delay(logic.targetAttr)
      headlineAttribute        = logic.groupByAttr
      extractedData            = raw.map { cm =>
                                   val aggregationValue = getCMAttributeStringValue(cm, logic.groupByAttr)
                                   val headlineValue    = getCMAttributeStringValue(cm, headlineAttribute)
                                   ExtractedDataForAggregation(aggregationValue, headlineValue)
                                 }.toList
      aggregationTitle        <-
        domainSchema.attributesTitle(domain).map(_.getOrElse(aggregationOriginalName, aggregationOriginalName))
      countingTitle           <-
        domainSchema.attributesTitle(domain).map(_.getOrElse(countingOriginalName, countingOriginalName))
      result                   = List(
                                   GridPropertyItemModel(
                                     key = headlineAttribute,
                                     title = aggregationTitle,
                                     dataType = GridPropertyDataTypeString(),
                                     filter = GridPropertyFilterModel(
                                       `type` = GridPropertyFilterTypeString(),
                                       values = extractedData
                                         .distinctBy(_.aggregationValue)
                                         .map(extractedItem => GridPropertyFilterValueString(extractedItem.headlineValue))
                                         .some
                                     ).some
                                   ),
                                   GridPropertyItemModel(
                                     key = "количество",
                                     title = s"""Количество экземпяров "$countingTitle"""",
                                     dataType = GridPropertyDataTypeNumber(),
                                     filter = None
                                   )
                                 ) -> extractedData
    } yield result

  override def makeGrid(
      queryData: DataForDBQuery,
      raw: Seq[ConceptMap],
      logic: AggregationLogic,
      total: Long,
      domain: Domain,
      offset: Int,
      limit: Int
  ): F[Option[GridWithDataRenderTypeResponseModel]] = for {
    (properties, extractedData) <- makeGridProperties(queryData, raw, logic, domain, _ => true)

    result <- if (logic.unique)
                for {
                  items <- raw.toList
                             .traverse { cm =>
                               properties
                                 .traverse(prop => getCMAttributeValue(cm, prop.key, domain).map(a => prop.key -> a))
                                 .map(_.groupMapReduce(_._1)(_._2)((_, v) => v))
                                 .map { m =>
                                   Map("id" -> GridPropertyFilterValueString(java.util.UUID.randomUUID().toString)) ++ m
                                 }

                             }
                } yield GridWithDataRenderTypeResponseModel(
                  renderType = IDataRenderTypeTable(),
                  properties = properties,
                  items = items,
                  total = total
                ).some
              else
                for {
                  aggregationName <- logic.groupByAttr.replaceAll("_iid", "").pure[F]
                  attrs           <- domainSchema.attributesTitle(domain)
                  aggregationTitle = attrs.getOrElse(aggregationName, aggregationName)
                  aggregationBy    = aggregationTitle
                  groupItems       =
                    extractedData
                      .groupBy(_.aggregationValue)
                      .toList
                      .sortWith((el1, el2) => el1._2.size > el2._2.size)
                  items            = groupItems
                                       .slice(offset, offset + limit)
                                       .map { case (key, value) =>
                                         Map("id" -> GridPropertyFilterValueString(java.util.UUID.randomUUID().toString)) ++
                                           properties.groupMapReduce(_.key) {
                                             case prop if prop.key == "количество" =>
                                               GridPropertyFilterValueNumber(value.size.toDouble)
                                             case _                                =>
                                               GridPropertyFilterValueString(value.headOption.map(_.headlineValue).getOrElse(key))
                                           }((_, v) => v)
                                       }
                } yield GridWithDataRenderTypeResponseModel(
                  renderType = IDataRenderTypeChart(),
                  properties = properties,
                  items = items,
                  title = s"Агрегация по $aggregationBy".some,
                  total = groupItems.size.toLong
                ).some
  } yield result

  override def getCMAttributeValue(
      cm: ConceptMap,
      attribute: String,
      domain: Domain
  ): F[GridPropertyFilterValue] =
    domainSchema.schemaAttributesType(domain).map(_.getOrElse(attribute, "string")).map { attrType =>
      val stringValue = getCMAttributeStringValue(cm, attribute)
      GridPropertyFilterValue.fromValueAndType(stringValue, attrType)
    }

  override def getCMAttributeStringValue(
      cm: ConceptMap,
      attribute: String
  ): String =
    if (attribute.endsWith("_iid")) cm.get(attribute.dropRight(4)).asThing().getIID
    else cm.get(attribute).asAttribute().getValue.toString

}
