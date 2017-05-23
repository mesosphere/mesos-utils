package mesosphere.mesos.protos

sealed trait TaskState
case object TaskStaging extends TaskState
case object TaskStarting extends TaskState
case object TaskRunning extends TaskState
case object TaskKilling extends TaskState
case object TaskFinished extends TaskState
case object TaskFailed extends TaskState
case object TaskKilled extends TaskState
case object TaskLost extends TaskState
case object TaskError extends TaskState
case object TaskDropped extends TaskState
case object TaskUnreachable extends TaskState
case object TaskGone extends TaskState
case object TaskGoneByOperator extends TaskState
case object TaskUnknown extends TaskState
