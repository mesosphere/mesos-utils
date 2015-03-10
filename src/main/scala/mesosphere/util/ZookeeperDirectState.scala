package mesosphere.util

import java.lang.Boolean
import java.util
import java.util.UUID
import java.util.concurrent.{ Callable, ExecutorService, Future => JFuture }
import java.util.logging.Logger
import javax.activation.UnsupportedDataTypeException

import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Await, Future, TimeoutException }
import scala.util.{ Failure, Success }

import com.google.protobuf.ByteString
import mesosphere.mesos.protos.StateProtos
import org.apache.curator.framework._
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.ZKPaths
import org.apache.mesos.state.{ State, Variable }

/**
  * A State implementation that persists data to Zookeeper in the form of a
  * mesosphere.marathon.StateProtos.Entry protocol buffer message.
  *
  * Unlike org.apache.mesos.state.ZooKeeperState, this class does not use the libmesos
  * native library for persistence. This avoids a JVM crashing issue present in
  * libmesos as of March 2015. After libmesos is patched and the fix can be ascertained,
  * this class will likely be deprecated.
  *
  * The crashing issue is documented in these tickets:
  * https://github.com/mesosphere/marathon/issues/834
  * https://issues.apache.org/jira/browse/MESOS-1795
  *
  */
class ZookeeperDirectState(
  zNode: String,
  serverList: String,
  sessionTimeout: Duration,
  connectionTimeout: Duration,
  executorService: ExecutorService)
    extends State {

  private val log = Logger.getLogger(getClass.getName)
  private implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(executorService)
  private val eventualCuratorClient: Future[CuratorFramework] = createZookeeperClient()

  /**
    * Block until we have a connection to Zookeeper. If the max time is reached
    * an exception will be thrown.
    * *
    * @param duration the max amount of time to wait
    */
  def awaitConnection(duration: Duration): Unit = {
    Await.ready(eventualCuratorClient, duration)
  }

  /**
    * Returns the client, if initialized and connected. Otherwise throws an exception
    */
  private def client: CuratorFramework = eventualCuratorClient.value match {
    case Some(Success(c))  => c
    case Some(Failure(ex)) => throw ex
    case None              => throw new TimeoutException("Haven't connected to Zookeeper yet")
  }

  /**
    * Unmarshall an Entry from Zookeeper and return it as an eventual Variable
    */
  override def fetch(name: String): JFuture[Variable] = jfuture {

    if (!exists(name)) {
      new LocalVariable(name)
    }
    else {
      val data: Array[Byte] = client.getData.forPath(fullPath(name))
      data.size match {
        case 0 =>
          new LocalVariable(name)
        case _ =>
          val entry = StateProtos.Entry.parseFrom(data)
          new LocalVariable(name, entry.getValue.toByteArray, Some(entry.getUuid.toStringUtf8))
      }
    }
  }

  /**
    * Marshall a variable to an Entry and store it in Zookeeper
    */

  override def store(variable: Variable): JFuture[Variable] = variable match {
    case v: LocalVariable =>
      store(v)
    case _ =>
      throw new UnsupportedDataTypeException("An unexpected variable was encountered for a Zookeeper operation")
  }

  private def store(lv: LocalVariable): JFuture[Variable] = jfuture {

    val persistedVariable = fetch(lv.name).get.asInstanceOf[LocalVariable]
    val inputMatchesPersistedVersion = persistedVariable.storedUuid == lv.storedUuid

    if (inputMatchesPersistedVersion) {

      val entryData = StateProtos.Entry.newBuilder()
        .setName(lv.name)
        .setUuid(ByteString.copyFromUtf8(UUID.randomUUID().toString))
        .setValue(ByteString.copyFrom(lv.value()))
        .build()
        .toByteArray

      mkDir(Some(lv.name))
      client.setData().forPath(fullPath(lv.name), entryData)
      fetch(lv.name).get()
    }
    else {
      log.severe(s"Attempt was made to persist ${lv.name} from obsolete version")
      null
    }

  }

  /**
    * Remove the variable from our Zookeeper node. Returns false if deletion was not necessary.
    */
  override def expunge(variable: Variable): JFuture[Boolean] = variable match {
    case v: LocalVariable =>
      expunge(v)
    case _ =>
      throw new UnsupportedDataTypeException("An unexpected variable was encountered for a Zookeeper operation")
  }

  private def expunge(lv: LocalVariable): JFuture[Boolean] = jfuture {

    if (exists(lv.name)) {
      client.delete().forPath(fullPath(lv.name))
      true
    }
    else false
  }

  /**
    * Return the children of a Zookeeper node
    */
  override def names: JFuture[util.Iterator[String]] = {
    jfuture { client.getChildren.forPath(zNode).iterator() }
  }

  private def fullPath(name: String) = s"$zNode/$name"

  private def mkDir(name: Option[String] = None, zkClient: CuratorFramework = client): Unit = {
    val path = name map fullPath getOrElse zNode
    ZKPaths.mkdirs(zkClient.getZookeeperClient.getZooKeeper, path)
  }

  private def exists(name: String): Boolean = {
    client.checkExists().forPath(fullPath(name)) != null
  }

  /**
    * Creates our Zookeeper client
    */
  private def createZookeeperClient(): Future[CuratorFramework] = {

    val RETRY_POLICY = new ExponentialBackoffRetry(1000, 4)

    val client = CuratorFrameworkFactory.newClient(
      serverList,
      sessionTimeout.toMillis.toInt,
      connectionTimeout.toMillis.toInt,
      RETRY_POLICY
    )

    client.start()

    // Make sure we have a successful connection before returning the client
    Future {
      val connected = client.getZookeeperClient.blockUntilConnectedOrTimedOut()
      if (!connected) throw new TimeoutException("Could not connect to Zookeeper")

      // Create our znode, if necessary
      mkDir(zkClient = client)

      client
    }
  }

  /**
    * Wraps a function literal with a Java Future using the executorService class variable
    */
  private def jfuture[A](f: => A): JFuture[A] = {
    executorService.submit(new Callable[A]() { def call(): A = f })
  }

}

/**
  * An implementation of the JNI Variable (used by State, among others). Not actually using JNI.
  */
class LocalVariable(
    val name: String,
    val data: Array[Byte] = Array[Byte](),
    val storedUuid: Option[String] = None) extends Variable {

  override def value(): Array[Byte] = data
  override def mutate(value: Array[Byte]): Variable = new LocalVariable(name, value, storedUuid)
  def isPersisted: Boolean = data.nonEmpty
}

