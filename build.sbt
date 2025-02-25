ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "db-zspark",
    idePackagePrefix := Some("dev.fernanluyano.dbzpark")
  )
