

libraryDependencies ++= Seq(
    "org.apache.hbase" % "hbase-client" % "1.0.0-cdh5.4.7",
    "org.apache.hbase" % "hbase-testing-util" % "1.0.0-cdh5.4.7" % Test,
    "junit" % "junit" % "4.11" % Test
)

resolvers += "cdh.repo" at "https://repository.cloudera.com/artifactory/cloudera-repos"