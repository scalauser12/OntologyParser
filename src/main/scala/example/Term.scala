package example

final case class Term(
    id: Long,
    name: String,
    msid: Long,
    msObjType: String,
    entrezGeneId: Option[Long],
    proteinClass: Option[String],
    medScanId: Option[Long],
    primaryCellLocalization: Option[String]
)
