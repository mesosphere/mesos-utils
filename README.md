# Mesos Utils

Scala utilities for building distributed systems on top of Mesos.

## Usage

### Maven

Add the Mesosphere repository and the dependency to your `pom.xml`:

    <properties>
        <mesos-utils.version>0.0.4</mesos-utils.version>
    </properties>
    ...
    <repositories>
        <repository>
            <id>mesosphere-repo</id>
            <name>Mesosphere Repo</name>
            <url>http://downloads.mesosphere.io/maven</url>
        </repository>
    </repositories>
    ...
    <dependencies>
        <dependency>
            <groupId>mesosphere</groupId>
            <artifactId>mesos-utils</artifactId>
            <version>${mesos-utils.version}</version>
        </dependency>
    </dependencies>

### SBT

Add this to your SBT config:

    resolvers += "Mesosphere Repo" at "http://downloads.mesosphere.io/maven"
    libraryDependencies += "mesosphere" % "mesos-utils" % "0.0.4"
