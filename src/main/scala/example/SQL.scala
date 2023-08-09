package example

import cats.effect._
import com.zaxxer.hikari.HikariConfig
import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import cats.implicits._

object SQL {
  def hikariTransactor(
      dbUser: String,
      passwd: String
  ): Resource[IO, HikariTransactor[IO]] = for {
    hikariConfig <- Resource.eval(IO {
      val config = new HikariConfig()
      config.setDriverClassName("org.postgresql.Driver")
      config.setJdbcUrl("jdbc:postgresql:OntologyData")
      config.setUsername(dbUser)
      config.setPassword(passwd)
      config
    })
    xa <- HikariTransactor.fromHikariConfig[IO](hikariConfig)
  } yield xa

  def insertOntologyData(
      data: Term
  )(implicit transactor: Transactor[IO]): IO[Unit] = {
    import data._
    fr"""insert into ontology_data (
        id,
        name,
        msid,
        ms_obj_type,
        entrez_gene_id,
        protein_class,
        medscan_id,
        primary_cell_localization
      ) values (
        $id,
        $name,
        $msid,
        $msObjType,
        $entrezGeneId,
        $proteinClass,
        $medScanId,
        $primaryCellLocalization
      )""".update.run.transact(transactor).void
  }

  def insertMedScanData(termiteId: String, medScanId: Long)(implicit
      transactor: Transactor[IO]
  ): IO[Unit] =
    fr"insert into medscan_data (termite_id, medscan_id) values ($termiteId, $medScanId)".update.run
      .transact(transactor)
      .void

  def populateIdMappingTable(implicit transactor: Transactor[IO]): IO[Unit] =
    fr"""insert
        |into termite_id_mapping(termite_id,
        |                        entrez_gene_id,
        |                        ms_obj_type,
        |                        protein_class,
        |                        primary_cell_localization)
        |select termite_id, entrez_gene_id, ms_obj_type, protein_class, primary_cell_localization
        |from medscan_data md
        |         join ontology_data od on md.medscan_id = od.id
        |order by id""".stripMargin.update.run.transact(transactor).void

  def populateProteinClassTable(implicit transactor: Transactor[IO]): IO[Unit] =
    fr"""insert into termite_to_protein_class(context, mapping_from, mapping_to)
        |select 'termite_to_protein_class', termite_id, protein_class
        |from termite_id_mapping
        |where protein_class is not null""".stripMargin.update.run
      .transact(transactor)
      .void

  def populatePrimaryLocalisationTable(implicit
      transactor: Transactor[IO]
  ): IO[Unit] =
    fr"""insert into termite_to_primary_cell_localization(context, mapping_from, mapping_to)
        |select 'termite_to_primary_cell_localization', termite_id, primary_cell_localization
        |from termite_id_mapping
        |where primary_cell_localization is not null""".stripMargin.update.run
      .transact(transactor)
      .void

  private def dropTable(name: String) =
    (fr"drop table if exists " ++ Fragment.const(name)).update.run

  def dropAllTables(implicit transactor: Transactor[IO]): IO[Int] =
    (
      dropTable("medscan_data"),
      dropTable("ontology_data"),
      dropTable("termite_id_mapping"),
      dropTable("termite_to_primary_cell_localization"),
      dropTable(("termite_to_protein_class"))
    )
      .mapN(_ + _ + _ + _ + _)
      .transact(transactor)

  def createAllTables(implicit transactor: Transactor[IO]): IO[Unit] =
    (
      createMedScanDataTable,
      createOntologyDataTable,
      createTermiteIdMappingTable,
      createTermiteToPrimaryCellLocalisationTable,
      createTermiteToProteinClassTable
    ).mapN(_ + _ + _ + _ + _).transact(transactor).void

  private val createMedScanDataTable =
    fr"""create table medscan_data
        |(
        |    termite_id text,
        |    medscan_id bigint
        |)""".stripMargin.update.run

  private val createOntologyDataTable =
    fr"""create table ontology_data
        |(
        |    id                        bigint,
        |    name                      text,
        |    msid                      bigint,
        |    ms_obj_type               text,
        |    entrez_gene_id            bigint,
        |    protein_class             text,
        |    medscan_id                bigint,
        |    primary_cell_localization text
        |)""".stripMargin.stripMargin.update.run

  private val createTermiteIdMappingTable =
    fr"""create table termite_id_mapping
        |(
        |    termite_id                text,
        |    entrez_gene_id            bigint,
        |    ms_obj_type               text,
        |    protein_class             text,
        |    primary_cell_localization text
        |)""".stripMargin.update.run

  private val createTermiteToPrimaryCellLocalisationTable =
    fr"""create table termite_to_primary_cell_localization
        |(
        |    context      text,
        |    mapping_from text,
        |    mapping_to   text
        |)""".stripMargin.update.run

  private val createTermiteToProteinClassTable =
    fr"""create table termite_to_protein_class
        |(
        |    context      text,
        |    mapping_from text,
        |    mapping_to   text
        |)""".stripMargin.update.run
}
