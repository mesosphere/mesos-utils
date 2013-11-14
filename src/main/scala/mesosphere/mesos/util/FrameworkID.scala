package mesosphere.mesos.util

/**
 * @author Tobi Knaup
 */

case class FrameworkID(value: String) {

  def toProto = {
    org.apache.mesos.Protos.FrameworkID.newBuilder
      .setValue(value)
      .build
  }
}
