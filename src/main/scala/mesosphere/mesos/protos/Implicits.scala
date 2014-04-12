package mesosphere.mesos.protos

import org.apache.mesos.Protos
import scala.collection.JavaConverters.{asJavaIterableConverter, iterableAsScalaIterableConverter}
import com.google.protobuf.ByteString

object Implicits {

  implicit def frameworkIDToProto(frameworkId: FrameworkID): Protos.FrameworkID = {
    Protos.FrameworkID.newBuilder
      .setValue(frameworkId.value)
      .build
  }

  implicit def frameworkIDToCaseClass(frameworkId: Protos.FrameworkID): FrameworkID = {
    FrameworkID(
      frameworkId.getValue
    )
  }

  implicit def frameworkInfoToProto(frameworkInfo: FrameworkInfo): Protos.FrameworkInfo = {
    Protos.FrameworkInfo.newBuilder
      .setId(frameworkInfo.id)
      .setName(frameworkInfo.name)
      .setUser(frameworkInfo.user)
      .setRole(frameworkInfo.role)
      .setCheckpoint(frameworkInfo.checkpoint)
      .setFailoverTimeout(frameworkInfo.failoverTimeout)
      .build
  }

  implicit def frameworkInfoToCaseClass(frameworkInfo: Protos.FrameworkInfo): FrameworkInfo = {
    FrameworkInfo(
      frameworkInfo.getName,
      frameworkInfo.getUser,
      frameworkInfo.getId,
      frameworkInfo.getFailoverTimeout,
      frameworkInfo.getCheckpoint,
      frameworkInfo.getRole
    )
  }

  implicit def rangeToProto(range: Range): Protos.Value.Range = {
    Protos.Value.Range.newBuilder
      .setBegin(range.begin)
      .setEnd(range.end)
      .build
  }

  implicit def rangeToCaseClass(range: Protos.Value.Range): Range = {
    Range(
      range.getBegin,
      range.getEnd
    )
  }

  implicit def resourceToProto(resource: Resource): Protos.Resource = {
    resource match {
      case RangesResource(name, ranges, role) => {
        val rangesProto = Protos.Value.Ranges.newBuilder
          .addAllRange(ranges.map(rangeToProto).asJava)
          .build
        Protos.Resource.newBuilder
          .setType(Protos.Value.Type.RANGES)
          .setName(name)
          .setRanges(rangesProto)
          .setRole(role)
          .build
      }
      case ScalarResource(name, value, role) => {
        Protos.Resource.newBuilder
          .setType(Protos.Value.Type.SCALAR)
          .setName(name)
          .setScalar(Protos.Value.Scalar.newBuilder.setValue(value))
          .setRole(role)
          .build
      }
      case SetResource(name, items, role) => {
        val set = Protos.Value.Set.newBuilder
          .addAllItem(items.asJava)
          .build
        Protos.Resource.newBuilder
          .setType(Protos.Value.Type.SET)
          .setName(name)
          .setSet(set)
          .setRole(role)
          .build
      }
      case unsupported: Resource => {
        throw new IllegalArgumentException(s"Unsupported type: $unsupported")
      }
    }
  }

  implicit def resourceToCaseClass(resource: Protos.Resource): Resource = {
    resource.getType match {
      case Protos.Value.Type.RANGES =>
        RangesResource(
          resource.getName,
          resource.getRanges.getRangeList.asScala.toSeq.map(rangeToCaseClass),
          resource.getRole
        )
      case Protos.Value.Type.SCALAR =>
        ScalarResource(
          resource.getName,
          resource.getScalar.getValue,
          resource.getRole
        )
      case Protos.Value.Type.SET =>
        SetResource(
          resource.getName,
          resource.getSet.getItemList.asScala.toSet,
          resource.getRole
        )
      case unsupported: Protos.Value.Type =>
        throw new IllegalArgumentException(s"Unsupported type: $unsupported")
    }
  }

  implicit def slaveIDToProto(slaveId: SlaveID): Protos.SlaveID = {
    Protos.SlaveID.newBuilder
      .setValue(slaveId.value)
      .build
  }

  implicit def slaveIDToCaseClass(slaveId: Protos.SlaveID): SlaveID = {
    SlaveID(
      slaveId.getValue
    )
  }

  implicit def taskIDToProto(taskId: TaskID): Protos.TaskID = {
    Protos.TaskID.newBuilder
      .setValue(taskId.value)
      .build
  }

  implicit def taskIDToCaseClass(taskId: Protos.TaskID): TaskID = {
    TaskID(
      taskId.getValue
    )
  }

  implicit def taskStateToProto(taskState: TaskState): Protos.TaskState = {
    taskState match {
      case TaskFailed => Protos.TaskState.TASK_FAILED
      case TaskFinished => Protos.TaskState.TASK_FINISHED
      case TaskKilled => Protos.TaskState.TASK_KILLED
      case TaskLost => Protos.TaskState.TASK_LOST
      case TaskRunning => Protos.TaskState.TASK_RUNNING
      case TaskStaging => Protos.TaskState.TASK_STAGING
      case TaskStarting => Protos.TaskState.TASK_STARTING
    }
  }

  implicit def taskStateToCaseClass(taskState: Protos.TaskState): TaskState = {
    taskState match {
      case Protos.TaskState.TASK_FAILED => TaskFailed
      case Protos.TaskState.TASK_FINISHED => TaskFinished
      case Protos.TaskState.TASK_KILLED => TaskKilled
      case Protos.TaskState.TASK_LOST => TaskLost
      case Protos.TaskState.TASK_RUNNING => TaskRunning
      case Protos.TaskState.TASK_STAGING => TaskStaging
      case Protos.TaskState.TASK_STARTING => TaskStarting
    }
  }

  implicit def taskStatusToProto(taskStatus: TaskStatus): Protos.TaskStatus = {
    Protos.TaskStatus.newBuilder
      .setTaskId(taskStatus.taskId)
      .setState(taskStatus.state)
      .setMessage(taskStatus.message)
      .setData(ByteString.copyFrom(taskStatus.data))
      .setSlaveId(taskStatus.slaveId)
      .setTimestamp(taskStatus.timestamp)
      .build
  }

  implicit def taskStatusToCaseClass(taskStatus: Protos.TaskStatus): TaskStatus = {
    TaskStatus(
      taskStatus.getTaskId,
      taskStatus.getState,
      taskStatus.getMessage,
      taskStatus.getData.toByteArray,
      taskStatus.getSlaveId,
      taskStatus.getTimestamp
    )
  }

  implicit def resourcesToProto(resources: Iterable[Resource]): java.lang.Iterable[Protos.Resource] = {
    resources.map(r => r: Protos.Resource).asJava
  }
}
