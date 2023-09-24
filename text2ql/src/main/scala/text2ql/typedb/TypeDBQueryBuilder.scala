package text2ql.typedb

import cats.effect.kernel._
import cats.implicits._
import text2ql.api.{AggregationLogic, DataForDBQuery, Domain}
import text2ql.service.DomainSchemaService

trait TypeDBQueryBuilder[F[_]] {

  def build(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      unlimitedQuery: Boolean,
      configLimit: Int,
      domain: Domain,
      countQuery: Boolean = false,
      countDistinctQuery: Boolean = false
  ): F[String]

}

object TypeDBQueryBuilder {

  def apply[F[_]: Sync](
      queryHelper: TypeDBQueryHelper[F],
      domainSchema: DomainSchemaService[F]
  ): Resource[F, TypeDBQueryBuilder[F]] =
    Resource.eval(Sync[F].delay(new TypeDBQueryBuilderImpl(queryHelper, domainSchema)))

}

class TypeDBQueryBuilderImpl[F[_]: Sync](queryHelper: TypeDBQueryHelper[F], domainSchema: DomainSchemaService[F])
    extends TypeDBQueryBuilder[F] {

  def build(
      queryData: DataForDBQuery,
      logic: AggregationLogic,
      unlimitedQuery: Boolean,
      configLimit: Int,
      domain: Domain,
      countQuery: Boolean = false,
      countDistinctQuery: Boolean = false
  ): F[String] = queryData.entityList
    .filter(_.includeGetClause)
    .traverse(e => domainSchema.headlineAttributes(domain).map(_.get(e.entityName)))
    .map(_.flatten)
    .flatMap { headlines =>
      val attributesToIncludeToQuery =
        if (countQuery) Seq.empty[String] else logic.groupByAttr +: logic.visualization.tags :++ headlines

      val entityClauseF = queryData.entityList.foldLeftM("match ") { (query, entity) =>
        val addEntity         = s"$$${entity.entityName} isa ${entity.entityName}; "
        val addAttributeNames =
          entity.attributes
            .filter { a =>
              a.attributeValues.exists(_.nonEmpty) || attributesToIncludeToQuery.contains(a.attributeName)
            }
            .distinctBy(_.attributeName)
            .map(a =>
              s"$$${entity.entityName} has ${if (a.attributeName.endsWith("_code") || a.attributeName.endsWith("_name") || a.attributeName.endsWith("_id"))
                a.attributeName.split("_").last
              else a.attributeName} $$${a.attributeName};"
            )
            .mkString(" ")
        val attributesClauseF =
          if (entity.attributes.isEmpty) "".pure[F]
          else queryHelper.collectAggregateClause(entity.attributes, domain)

        attributesClauseF.map { attributesClause =>
          query + addEntity + addAttributeNames + attributesClause
        }
      }

      val relationClauseF = queryData.relationList.foldLeftM("") { (query, relation) =>
        val addRelation       =
          s"$$${relation.relationName} (${relation.entities.map(e => s"$$$e").mkString(", ")}) isa ${relation.relationName}; "
        val addAttributeNames = relation.attributes
          .collect {
            case a if a.attributeValues.exists(_.nonEmpty) || attributesToIncludeToQuery.contains(a.attributeName) =>
              s"$$${relation.relationName} has ${a.attributeName} $$${a.attributeName};"
          }
          .mkString(" ")
        val attributesClauseF =
          if (relation.attributes.isEmpty) "".pure[F]
          else queryHelper.collectAggregateClause(relation.attributes, domain)

        attributesClauseF.map { attributesClause =>
          query + addRelation + addAttributeNames + attributesClause
        }
      }
      val getClause       = "get " + {
        queryData.entityList
          .filter(_.includeGetClause)
          .filter(e => if (countDistinctQuery) e.isTargetEntity else true)
          .map(_.entityName) ++ queryData.entityList
          .flatMap(_.attributes)
          .filter(a => attributesToIncludeToQuery.contains(a.attributeName))
          .map(_.attributeName) ++ queryData.relationList
          .filter(r => if (countDistinctQuery) r.isTargetRelation else r.includeGetClause)
          .map(_.relationName) ++ queryData.relationList
          .flatMap(_.attributes)
          .filter(a => attributesToIncludeToQuery.contains(a.attributeName))
          .map(_.attributeName)
      }.distinct.map(s => s"$$$s").mkString(", ") + ";"
      val offsetClause    =
        if (unlimitedQuery) ""
        else {
          if (logic.unique) {
            queryData.pagination
              .flatMap(_.page)
              .fold("")(page => s"offset ${page * queryData.pagination.flatMap(_.perPage).getOrElse(configLimit)};")
          } else ""
        }
      val limitClause     =
        if (unlimitedQuery) ""
        else {
          if (logic.unique)
            queryData.pagination.flatMap(_.perPage).fold(s"limit $configLimit;")(limit => s"limit $limit;")
          else ""
        }

      for {
        entityClause   <- entityClauseF
        relationClause <- relationClauseF
        resultQuery     = entityClause + relationClause + getClause + offsetClause + limitClause
      } yield resultQuery
    }

}
