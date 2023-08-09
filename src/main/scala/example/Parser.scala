package example

import cats.effect._
import fs2._
import fs2.io.file._

import scala.util.Try

object Parser {
  private val entrezGeneIdName = "Entrez_GeneID"
  private val proteinClassName = "Class"
  private val medScanIdName = "MedScan_ID"
  private val primaryCellLocalizationName = "Primary_Cell_Localization"


  def stream(filePath: String): Stream[IO, Term] =
    Files[IO]
      .readAll(Path(filePath))
      .through(text.utf8.decode)
      .through(text.lines)
      .split(_ == "")
      .map(_.toList)
      .filter(_.headOption.contains("[Term]"))
      .map(Parser.getTerm)
      .unNone

  private def getTerm(lines: List[String]) = {
    val entrezGeneId = extractLongXref(entrezGeneIdName, lines)
    val proteinClass = extractStringXref(proteinClassName, lines)
    val medScanId = extractLongXref(medScanIdName, lines)
    val primaryCellLocalization =
      extractStringXref(primaryCellLocalizationName, lines)

    for {
      id <- extractLongField("id", lines)
      name <- extractStringField("name", lines)
      msId <- extractLongField("msid", lines)
      msObjType <- extractStringField("msobjtype", lines)
    } yield Term(
      id,
      name,
      msId,
      msObjType,
      entrezGeneId,
      proteinClass,
      medScanId,
      primaryCellLocalization
    )
  }

  private def extractStringXref(name: String, lines: List[String]) =
    lines.collectFirst {
      case line if line.matches(s"^xref:\\s+$name\\s.*") =>
        line
          .replaceAll(s"^xref:\\s+$name\\s+", "")
          .replaceAll("^\"", "")
          .replaceAll("\"$", "")
    }

  private def extractLongXref(name: String, lines: List[String]) =
    Try(extractStringXref(name, lines).map(_.toLong)).toOption.flatten

  private def extractLongField(name: String, lines: List[String]) =
    Try(extractStringField(name, lines).map(_.toLong)).toOption.flatten

  private def extractStringField(name: String, lines: List[String]) =
    lines.collectFirst {
      case line if line.matches(s"^$name:\\s.*") =>
        line.replaceAll(s"^$name:\\s+", "")
    }
}
