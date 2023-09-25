package text2ql.migration

import java.time.Instant
import java.util.UUID

sealed trait HrEntity extends Product with Serializable

case class Employee(
    id: UUID,
    jobId: UUID,
    departmentId: UUID,
    gender: Boolean,
    name: String,
    email: String,
    hiredDate: Instant,
    fired: Boolean,
    firedDate: Option[Instant],
    path: String
) extends HrEntity

case class Region(
    id: UUID,
    code: String,
    name: String
) extends HrEntity
