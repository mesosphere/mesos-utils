resolvers += Classpaths.typesafeReleases

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.8.4")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")

// publishing

resolvers += "Era7 maven releases" at "http://releases.era7.com.s3.amazonaws.com"

addSbtPlugin("ohnosequences" % "sbt-s3-resolver" % "0.11.0")
