package mesosphere.mesos.protos

trait TaskState
object TaskStaging extends TaskState
object TaskStarting extends TaskState
object TaskRunning extends TaskState
object TaskFinished extends TaskState
object TaskFailed extends TaskState
object TaskKilled extends TaskState
object TaskLost extends TaskState
