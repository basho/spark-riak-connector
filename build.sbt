/**
  * Copyright (c) 2015-2016 Basho Technologies, Inc.
  *
  * This file is provided to you under the Apache License,
  * Version 2.0 (the "License"); you may not use this file
  * except in compliance with the License.  You may obtain
  * a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */

import sbt.Keys._

import scala.util.Properties

lazy val namespace = "spark-riak-connector"

//scalastyle:off
lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(publish := {})
  .aggregate(sparkRiakConnector,examples,sparkRiakConnectorTestUtils)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val sparkRiakConnector = (project in file("connector"))
  .settings(commonSettings: _*)
  .settings(commonDependencies: _*)
  .settings(name := namespace)
  .settings(
    assemblyJarName in assembly := s"$namespace-${version.value}-uber.jar",
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assembleArtifact in assemblyPackageDependency := true,
    test in assembly := {},
    artifact in (Compile, assembly) ~= { art =>
      art.copy(`classifier` = Some("uber"))
    },
    fork in Test := true,
    customMergeStrategy)
  .settings(publishSettings)
  .settings(addArtifact(artifact in (Compile, assembly), assembly).settings: _*)
  .dependsOn(sparkRiakConnectorTestUtils)
  .enablePlugins(AssemblyPlugin)

lazy val examples = (project in file("examples"))
  .settings(commonSettings: _*)
  .settings(commonDependencies: _*)
  .settings(
    name := s"$namespace-examples",
    libraryDependencies ++= Seq(
      "org.apache.spark"              %% "spark-streaming-kafka" % Versions.spark % "provided",
      "org.apache.kafka"              %% "kafka"                 % Versions.kafka,
      "org.jfree"                     %  "jfreechart"            % Versions.jfree,
      "com.github.wookietreiber"      %% "scala-chart"           % Versions.scalaChart))
  .settings(publishSettings)
  .dependsOn(sparkRiakConnector, sparkRiakConnectorTestUtils)
  .disablePlugins(AssemblyPlugin)

lazy val sparkRiakConnectorTestUtils = (project in file("test-utils"))
  .settings(commonSettings: _*)
  .settings(commonDependencies: _*)
  .settings(name := s"$namespace-test-utils")
  .settings(publishSettings)
  .disablePlugins(AssemblyPlugin)

lazy val commonSettings = Seq(
  organization := "com.basho.riak",
  version := "1.5.2-SNAPSHOT",
  scalaVersion := "2.10.6",
  crossPaths := true,
  spName := s"basho/$namespace",
  sparkVersion := Versions.spark,
  sparkComponents += "sql",
  spIgnoreProvided := true,
  parallelExecution in Test := false,
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "--exclude-categories=com.basho.riak.spark.rdd.RiakKVNotAvailableFeaturesTest"),
  scalacOptions in (Compile,doc) := Seq("-groups", "-implicits"),
  crossScalaVersions := Seq("2.10.6", "2.11.7"),
  aggregate in doc := true
)

lazy val commonDependencies = Seq(
  libraryDependencies ++= Seq(
      "com.basho.riak"               %  "dataplatform-riak-client"  % Versions.riakClient,
      "org.apache.spark"             %% "spark-sql"                 % Versions.spark % "provided",
      "org.apache.spark"             %% "spark-streaming"           % Versions.spark % "provided",
      "com.fasterxml.jackson.module" %% "jackson-module-scala"      % Versions.jacksonModule exclude("com.google.guava", "guava"),
      "net.javacrumbs.json-unit"     %  "json-unit"                 % Versions.jsonUnit exclude("com.fasterxml.jackson.module", "jackson-module-scala"),
      "junit"                        %  "junit"                     % Versions.junit,
      "org.hamcrest"                 %  "hamcrest-all"              % Versions.hamrest % "test",
      "org.mockito"                  %  "mockito-core"              % Versions.mockito % "test",
      "org.powermock"                %  "powermock-module-junit4"   % Versions.powermokc % "test",
      "org.powermock"                %  "powermock-api-mockito"     % Versions.powermokc % "test",
      "com.novocode"                 %  "junit-interface"           % Versions.junitInterface % "test",
      "com.basho.riak.test"          %  "riak-test-docker"          % Versions.riakTestDocker % "test" excludeAll (
        ExclusionRule("commons-logging","commons-logging"),
        ExclusionRule(organization = "com.fasterxml.jackson.core"),
        ExclusionRule(organization = "com.fasterxml.jackson.datatype"))
    ),

  resolvers := {
    val artifactory = "https://basholabs.artifactoryonline.com/basholabs"
    Seq(
      "Local Maven Repo" at "file:///" + Path.userHome + "/.m2/repository",
      "Basho Bintray Repo" at "https://dl.bintray.com/basho/data-platform",
      "Artifactory Realm snapshot" at s"$artifactory/libs-snapshot-local",
      "Artifactory Realm release" at s"$artifactory/libs-release-local"
    )
  }
)

lazy val customMergeStrategy = assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "reference.conf"                              => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

pomExtra := (
  <url>https://github.com/basho/spark-riak-connector</url>
  <licenses>
    <license>
      <name>The Apache Software License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
      <comments>A business-friendly OSS license</comments>
    </license>
  </licenses>

  <developers>
    <developer>
      <name>Sergey Galkin</name>
      <email>sgalkin@basho.com</email>
      <organization>Basho Technologies, Inc</organization>
      <organizationUrl>http://www.basho.com</organizationUrl>
    </developer>
    <developer>
      <name>Oleg Rocklin</name>
      <email>orocklin@basho.com</email>
      <organization>Basho Technologies, Inc</organization>
      <organizationUrl>http://www.basho.com</organizationUrl>
    </developer>
  </developers>

  <scm>
    <connection>scm:git:https://github.com/basho/spark-riak-connector.git</connection>
    <developerConnection>scm:git:ssh://github.com/basho/spark-riak-connector.git</developerConnection>
    <url>https://github.com/basho/spark-riak-connector</url>
    <tag>HEAD</tag>
  </scm>)

//workaround to prevent packaging empty jars for `root` project
Keys.`package` := {
  (Keys.`package` in (sparkRiakConnector, Compile)).value
  (Keys.`package` in (examples, Compile)).value
}

Keys.packageDoc := {
  (Keys.packageDoc in (sparkRiakConnector, Compile)).value
  (Keys.packageDoc in (examples, Compile)).value
}

Keys.packageSrc := {
  (Keys.packageSrc in (sparkRiakConnector, Compile)).value
  (Keys.packageSrc in (examples, Compile)).value
}

addCommandAlias("runIntegrationTests", "testOnly -- --include-categories=com.basho.riak.spark.rdd.IntegrationTests")
addCommandAlias("runRegressionTests", "testOnly -- --include-categories=com.basho.riak.spark.rdd.RegressionTests")
addCommandAlias("runRiakKVTests", "testOnly -- --include-categories=com.basho.riak.spark.rdd.RiakKVTests")
addCommandAlias("runRiakTSTests", "testOnly -- --include-categories=com.basho.riak.spark.rdd.RiakTSTests")
addCommandAlias("runNonIntegrationTests", "testOnly -- --exclude-categories=com.basho.riak.spark.rdd.IntegrationTests")

lazy val publishSettings = Seq(
  publishTo := {
    val artifactory = "https://basholabs.artifactoryonline.com/basholabs"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("Artifactory Realm" at s"$artifactory/libs-snapshot-local")
    else
      Some("Artifactory Realm" at s"$artifactory/libs-release-local")
  },
  credentials := Seq(Credentials("Artifactory Realm", "basholabs.artifactoryonline.com",
    Properties.envOrElse("ARTIFACTORY_USER", ""), Properties.envOrElse("ARTIFACTORY_PASS", ""))),
  publishMavenStyle := true
)