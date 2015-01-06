name := "cassandra-gdelt"

val phantomVersion = "1.4.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "twitter-repo" at "http://maven.twttr.com",
  "websudos-repo" at "http://maven.websudos.co.uk/ext-release-local"
)

libraryDependencies ++= Seq(
  "com.github.marklister" %% "product-collections" % "1.2.0",
  "org.capnproto"         % "runtime"           % "0.1.0",
  "com.websudos"         %% "phantom-dsl"       % phantomVersion,
  "com.websudos"         %% "phantom-zookeeper" % phantomVersion
)

resolvers += "Pellucid Bintray" at "http://dl.bintray.com/pellucid/maven"

libraryDependencies += "com.pellucid" %% "framian" % "0.3.3"