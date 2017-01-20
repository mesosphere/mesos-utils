import sbt._
import Keys._
import sbtrelease.ReleasePlugin._
import com.typesafe.sbt.SbtScalariform._
import ohnosequences.sbt.SbtS3Resolver.S3Resolver
import ohnosequences.sbt.SbtS3Resolver.{ s3, s3resolver }
import scalariform.formatter.preferences._

object MesosUtilsBuild extends Build {
  lazy val root = Project(
    id = "mesos-utils",
    base = file("."),
    settings = baseSettings ++ releaseSettings ++ publishSettings ++ formatSettings ++ Seq(
      libraryDependencies ++= Dependencies.root
    )
  )

  lazy val baseSettings = Defaults.defaultSettings ++ Seq (
    organization := "mesosphere",
    scalaVersion := "2.11.7",
    scalacOptions in Compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.6", "-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", "1.6", "-target", "1.6", "-Xlint:unchecked", "-Xlint:deprecation"),
    resolvers ++= Seq(
      "Apache Public Repo"     at "https://repository.apache.org/content/repositories/releases",
      "Mesosphere Public Repo" at "http://downloads.mesosphere.io/maven"
    )
  )

  lazy val publishSettings = S3Resolver.defaults ++ Seq(
    publishMavenStyle := true,
    publishTo := Some(s3resolver.value(
      "Mesosphere Public Repo (S3)",
      s3("downloads.mesosphere.io/maven")
    ))
  )

  lazy val formatSettings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences()
      .setPreference(IndentWithTabs, false)
      .setPreference(IndentSpaces, 2)
      .setPreference(AlignParameters, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(MultilineScaladocCommentsStartOnFirstLine, false)
      .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)
      .setPreference(PreserveDanglingCloseParenthesis, true)
      .setPreference(CompactControlReadability, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(PreserveSpaceBeforeArguments, true)
      .setPreference(SpaceBeforeColon, false)
      .setPreference(SpaceInsideBrackets, false)
      .setPreference(SpaceInsideParentheses, false)
      .setPreference(SpacesWithinPatternBinders, true)
      .setPreference(FormatXml, true)
    )
}

object Dependencies {
  import Dependency._

  val root = Seq(
    // runtime
    mesos % "compile",

    // test
    Test.scalatest % "test"
  )
}

object Dependency {
  object V {
    // runtime deps versions
    val Mesos = "1.0.2"

    // test deps versions
    val ScalaTest = "2.2.1"
  }

  val mesos = "org.apache.mesos" % "mesos" % V.Mesos

  object Test {
    val scalatest = "org.scalatest" %% "scalatest" % V.ScalaTest
  }
}

