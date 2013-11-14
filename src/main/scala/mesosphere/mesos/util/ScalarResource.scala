package mesosphere.mesos.util

/**
 * @author Tobi Knaup
 */

case class ScalarResource(name: String, value: Double, role: String = "*") {

  def toProto = {
    org.apache.mesos.Protos.Resource.newBuilder
      .setType(org.apache.mesos.Protos.Value.Type.SCALAR)
      .setName(name)
      .setScalar(org.apache.mesos.Protos.Value.Scalar.newBuilder.setValue(value))
      .setRole(role)
      .build
  }
}
