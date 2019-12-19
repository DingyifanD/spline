package za.co.absa.spline.consumer.service.internal.model

import java.util.UUID

case class Operation (
  _id: String,
  schema: Array[UUID],
  extra: Map[String, Any],
  params: Map[String, Any],
  childIds: Seq[String]
) {
  def this() = this(null, null, null, null, null)
}


