package text2ql.api

case class BuildQueryDTO(
    generalQuery: String,
    countQuery: String,
    aggregation: Boolean,
    numericDescriptionQuery: Option[String] = None
)
