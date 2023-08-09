package example

import cats.effect._
import cats.implicits._

object Main extends IOApp.Simple {
  // set user name and password
  private val dbUser = "user"
  private val passwd = "passwd"

  // set paths to Source files
  private val ontologyFilePath =
    "pathToOntologyFile"
  private val medScanFilePath =
    "pathToMedScanFile"

  override def run: IO[Unit] =
    SQL.hikariTransactor(dbUser, passwd).use { implicit transactor =>
      val parserIO =
        Parser
          .stream(ontologyFilePath)
          .evalMap(SQL.insertOntologyData)
          .compile
          .drain

      val csvIO = CSV
        .stream(medScanFilePath)
        .evalMap((SQL.insertMedScanData _).tupled)
        .compile
        .drain

      for {
        _ <- SQL.dropAllTables
        _ <- SQL.createAllTables
        _ <- (parserIO, csvIO).parTupled.void
        _ <- SQL.populateIdMappingTable
        _ <- SQL.populateProteinClassTable
        _ <- SQL.populatePrimaryLocalisationTable
      } yield ()
    }
}
