package text2ql.api

case class GeneralQueryDTO(headers: Vector[String], data: List[Vector[String]])

case class NumericDescriptionQueryDTO(
    maxValue: Double,
    minValue: Double,
    avgValue: Double,
    medianValue: Double,
    sumValue: Double
)
