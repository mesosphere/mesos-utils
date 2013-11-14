package mesosphere.mesos.util


/**
 * @author Tobi Knaup
 */

case class FrameworkInfo(name: String,
                         user: String = "", // Let Mesos assign the user
                         frameworkId: Option[FrameworkID] = None,
                         role: String = "*", // Default role
                         checkpoint: Boolean = false,
                         failoverTimeout: Double = 0.0d) {

  def toProto = {
    val builder = org.apache.mesos.Protos.FrameworkInfo.newBuilder
      .setName(name)
      .setUser(user)
      .setRole(role)
      .setCheckpoint(checkpoint)
      .setFailoverTimeout(failoverTimeout)

    frameworkId.map(id => builder.setId(id.toProto))
    builder.build
  }
}
