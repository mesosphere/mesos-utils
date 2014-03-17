package mesosphere.mesos.util

/**
 * @author Tobi Knaup
 */

case class TaskID(value: String) {

  def toProto = {
    org.apache.mesos.Protos.TaskID.newBuilder()
      .setValue(value)
      .build
  }
}
