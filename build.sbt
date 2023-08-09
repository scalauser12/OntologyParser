ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

val fs2DataVersion = "1.8.0"
val fs2Version = "3.7.0"
val http4sVersion = "1.0.0-M40"
val doobieVersion = "1.0.0-RC4"

lazy val root = (project in file("."))
  .settings(
    name := "OntologyParser",
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "org.gnieh" %% "fs2-data-csv" % fs2DataVersion,
      "org.gnieh" %% "fs2-data-csv-generic" % fs2DataVersion,
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-hikari" % doobieVersion,
      "org.tpolecat" %% "doobie-postgres" % doobieVersion
    )
  )
