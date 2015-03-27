# Mesos Utils

Scala utilities for building distributed systems on top of Mesos.

## Usage

We cut releases for each recent Mesos version. Current releases are:

* Mesos `0.16.0` - mesos-utils `0.16.0-2`
* Mesos `0.17.0` - mesos-utils `0.17.0-2`
* Mesos `0.18.2` - mesos-utils `0.18.2-2`
* Mesos `0.19.0` - mesos-utils `0.19.0-1`
* Mesos `0.20.0` - mesos-utils `0.20.0-2` (cross-built for Scala 2.10 and 2.11)
* Mesos `0.20.1` - mesos-utils `0.20.1-1` (cross-built for Scala 2.10 and 2.11)
* Mesos `0.21.0` - mesos-utils `0.21.0-1` (cross-built for Scala 2.10 and 2.11)
* Mesos `0.21.1` - mesos-utils `0.21.1`   (cross-built for Scala 2.10 and 2.11)
* Mesos `0.22.0` - mesos-utils `0.22.0-1` (cross-built for Scala 2.10 and 2.11)

### Maven

Add the Mesosphere repository and the dependency to your `pom.xml`:

    <properties>
        <mesos-utils.version>0.22.0-1</mesos-utils.version>
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
            <artifactId>mesos-utils_2.11</artifactId>
            <version>${mesos-utils.version}</version>
        </dependency>
    </dependencies>

### SBT

Add this to your SBT config:

    resolvers += "Mesosphere Repo" at "http://downloads.mesosphere.io/maven"
    libraryDependencies += "mesosphere" %% "mesos-utils" % "0.22.0-1"

## Developing

To deploy a new version of `mesos-utils`, simply run

```bash
$ sbt release
```

and follow the prompts.
