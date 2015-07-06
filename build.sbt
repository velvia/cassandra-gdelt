name := "cassandra-gdelt"

val phantomVersion = "1.4.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Velvia Bintray" at "https://dl.bintray.com/velvia/maven",
  "twitter-repo" at "http://maven.twttr.com",
  "websudos-repo" at "http://maven.websudos.co.uk/ext-release-local"
)

libraryDependencies ++= Seq(
  "com.opencsv"           % "opencsv"           % "3.3",
  // "org.capnproto"         % "runtime"           % "0.1.0",
  "org.velvia.filo"      %% "filo-scala"        % "0.1.3",
  "com.websudos"         %% "phantom-dsl"       % phantomVersion,
  "com.websudos"         %% "phantom-zookeeper" % phantomVersion
)
