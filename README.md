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
* Mesos `0.22.1` - mesos-utils `0.22.1-1` (cross-built for Scala 2.10 and 2.11)
* Mesos `0.23.0` - mesos-utils `0.23.0` (cross-built for Scala 2.10 and 2.11)
* Mesos `0.24.0` - mesos-utils `0.24.0` (cross-built for Scala 2.10 and 2.11)
* Mesos `0.25.0` - mesos-utils `0.25.0` (cross-built for Scala 2.10 and 2.11)

### Maven

Add the Mesosphere repository and the dependency to your `pom.xml`:

```xml
<properties>
    <mesos-utils.version>0.25.0</mesos-utils.version>
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
```

### SBT

Add this to your SBT config:

```scala
resolvers += "Mesosphere Repo" at "http://downloads.mesosphere.io/maven"
libraryDependencies += "mesosphere" %% "mesos-utils" % "0.25.0"
```

## Developing

To deploy a new version of `mesos-utils`, simply run

```bash
$ sbt release
```

and follow the prompts.

## Local publish

### local sbt repo

To publish a new version of `mesos-utils` into local sbt repo, simply run

```bash
$ sbt publish-local
```

then it will be published (default) to

`${HOME}/.ivy2/local/mesosphere/mesos-utils_${SCALA_VERSION}/${VERSION}`

Thus you can add the dependency into your sbt configuration.

### local maven repo

To publish a new version of `mesos-utils` into local maven repo, simply run

```bash
$ sbt publishM2
```

then it will be published (default) to

`${HOME}/.m2/repository/mesosphere/mesos-utils_${SCALA_VERSION}/${VERSION}/`

Thus you can add the dependency in pom.xml of your maven project.
