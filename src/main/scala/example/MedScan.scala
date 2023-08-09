package example

import fs2.data.csv.RowDecoder
import fs2.data.csv.generic.semiauto.deriveRowDecoder

import scala.util.Try

final case class MedScan(
    hitId: String,
    field2: String,
    medScanId: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String
) {
  def id: Option[Long] =
    Try(medScanId.replaceAll("^MSCAN_", "").toLong).toOption
}

object MedScan {
  implicit val myRowDecoder: RowDecoder[MedScan] = deriveRowDecoder
}
