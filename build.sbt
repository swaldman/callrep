ThisBuild / organization := "com.mchange"
ThisBuild / version      := "0.0.1-SNAPSHOT"

ThisBuild / resolvers += Resolver.sonatypeRepo("releases")
ThisBuild / resolvers += Resolver.sonatypeRepo("snapshots")
ThisBuild / resolvers += Resolver.mavenLocal

ThisBuild / publishTo := {
  if (isSnapshot.value) Some(Resolver.sonatypeRepo("snapshots")) else Some(Resolver.url("sonatype-staging", url("https://oss.sonatype.org/service/local/staging/deploy/maven2")))
}

lazy val root = project
  .in(file("."))
  .settings(
    name                := "callrep",
    scalaVersion        := "2.11.12",
    libraryDependencies += "com.mchange"            %% "superflex"             % "0.2.2-SNAPSHOT",
    libraryDependencies += "com.mchange"            %% "mchange-commons-scala" % "0.4.16",
    libraryDependencies += "com.mchange"            %% "texttable"             % "0.0.2",
    libraryDependencies += "com.mchange"            %% "mlog-scala"            % "0.3.14",
    libraryDependencies += "org.scala-lang.modules" %% "scala-xml"             % "1.3.0",
    libraryDependencies += "com.mysql"               % "mysql-connector-j"     % "8.0.31",
    libraryDependencies += "org.postgresql"          % "postgresql"            % "42.5.0",
    scalacOptions       += "-deprecation",
    javaOptions         += "-Xmx25G",
    fork                := true,
    pomExtra := pomExtraForProjectName_LGPLv21( name.value )
  )


// publication, pom extra stuff, note this is single-licensed under LGPL v2.1

def pomExtraForProjectName_LGPLv21( projectName : String ) = {
    <url>https://github.com/swaldman/{projectName}</url>
    <licenses>
      <license>
        <name>GNU Lesser General Public License, Version 3</name>
        <url>https://www.gnu.org/licenses/lgpl-2.1.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>https://github.com/swaldman/{projectName}</url>
      <connection>scm:git:git@github.com:swaldman/{projectName}.git</connection>
    </scm>
    <developers>
      <developer>
        <id>swaldman</id>
        <name>Steve Waldman</name>
        <email>swaldman@mchange.com</email>
      </developer>
    </developers>
}



