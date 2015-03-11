package mesosphere.util

import java.io.IOException
import java.lang.Boolean
import java.util
import java.util.UUID
import java.util.concurrent.{ Callable, Future => JFuture, FutureTask }

import scala.concurrent.{ ExecutionContext, Future => SFuture, Promise }

import com.google.protobuf.ByteString
import mesosphere.mesos.protos.StateProtos
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{ BackgroundCallback, CuratorEvent }
import org.apache.curator.utils.ZKPaths
import org.apache.log4j.Logger
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
  * The State methods return a subtype of Java's Future that implements ScalaFutureSupport
  * and contains a Scala Future which may eventually contain the expected value.
  */
trait ScalaFutureSupport[A] {
  def future: SFuture[A]
}

class ZookeeperDirectState(zNode: String, client: CuratorFramework, implicit val context: ExecutionContext)
    extends State with ZKBackgroundPromiseSupport {

  private val log = Logger.getLogger(getClass.getName)
  private val MAX_ZK_SIZE_IN_BYTES = 0xfffff

  /**
    * Fetches the data from ZK. Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  override def fetch(name: String): JFuture[Variable] = {
    handleFetch(name, new GetDataCallbackPromise(name)).asScalaFutureWrapper
  }

  /**
    * Marshall a variable to an Entry and store it in ZK.
    * Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  override def store(variable: Variable): JFuture[Variable] = {

    if (variable.value().size > MAX_ZK_SIZE_IN_BYTES) {
      throw new IOException("Serialized data is too big (> 1 MB)")
    }
    handleStore(variable.asLocal).asScalaFutureWrapper
  }

  /**
    * Remove the variable from ZK. Returns false if deletion was not necessary.
    * Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  override def expunge(variable: Variable): JFuture[Boolean] = {
    handleExpunge(variable.asLocal).asScalaFutureWrapper
  }

  /**
    * Returns the children of our ZK node.
    * Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  override def names: JFuture[util.Iterator[String]] = {
    handleNames.asScalaFutureWrapper
  }

  /**
    * Fetches the data from ZK. Returns an instance of ScalaFutureSupport type containing a Scala-Future.
    * Uses a given callback to convert Curator's result to a Promise and then a Scala Future.
    */
  def handleFetch(name: String, callback: GetDataCallbackPromise): SFuture[Variable] = {

    if (name == null) {
      throw new RuntimeException("Whoah, name is null!")
    }

    exists(name) match {
      case java.lang.Boolean.TRUE  => client.getData.inBackground(callback).forPath(fullPath(name))
      case java.lang.Boolean.FALSE => callback.success(new LocalVariable(name))
    }

    callback.future
  }

  /**
    * Marshall a variable to an Entry and store it in ZK.
    *
    * First fetches a variable and then flat maps its Scala future
    * as the second argument to store(LocalVariable, LocalVariable)
    * so it can ensure the right version is being updated.
    *
    * Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  private def handleStore(lv: LocalVariable): SFuture[Variable] = {

    implicit class JFutureToSFuture[A](f: JFuture[A]) {
      def asScala: SFuture[A] = f match {
        case sf: ScalaFutureSupport[A] => sf.future
      }
    }

    // Grab the latest persisted variable and flatMap its future to store()'s future
    val persistedVariableFuture: SFuture[Variable] = fetch(lv.name).asScala
    val storeFuture: SFuture[Variable] = persistedVariableFuture.flatMap { pv: Variable =>
      Option(handleStore(lv, pv.asLocal)) getOrElse SFuture(null)
    }

    storeFuture
  }

  /**
    * Marshall a variable to an Entry and store it in ZK.
    * Compares the variable with the latest persisted one to prevent an obsolete
    * variable from being persisted.
    *
    * Returns null if the new veriable cannot be persisted
    */
  private def handleStore(lv: LocalVariable, persistedVariable: LocalVariable): SFuture[Variable] = {

    val inputMatchesPersistedVersion: Boolean = persistedVariable.storedUuid == lv.storedUuid

    if (inputMatchesPersistedVersion) {

      val entryData = StateProtos.Entry.newBuilder()
        .setName(lv.name)
        .setUuid(ByteString.copyFromUtf8(UUID.randomUUID().toString))
        .setValue(ByteString.copyFrom(lv.value()))
        .build()
        .toByteArray

      mkDir(Some(lv.name))

      val callback = new SetDataCallbackPromise(lv.name)
      client.setData().inBackground(callback).forPath(fullPath(lv.name), entryData)
      callback.future
    }
    else {
      log.error(s"Attempt was made to persist ${lv.name} from obsolete version")
      null
    }
  }

  /**
    * Remove the variable from ZK. Returns false if deletion was not necessary.
    * Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  private def handleExpunge(lv: LocalVariable): SFuture[Boolean] = {

    val callback = new ExpungeCallbackPromise()

    if (exists(lv.name)) {
      client.delete().inBackground(callback).forPath(fullPath(lv.name))
    }
    else {
      callback.success(false)
    }

    callback.future
  }

  /**
    * Returns the children of our ZK node.
    * Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  def handleNames: SFuture[util.Iterator[String]] = {
    val callback = new NamesCallbackPromise
    client.getChildren.inBackground(callback).forPath(zNode)
    callback.future
  }

  private def fullPath(name: String) = s"$zNode/$name"

  private def mkDir(name: Option[String] = None, zkClient: CuratorFramework = client): Unit = {
    val path = name map fullPath getOrElse zNode
    ZKPaths.mkdirs(zkClient.getZookeeperClient.getZooKeeper, path)
  }

  private def exists(name: String): Boolean = {
    client.checkExists().forPath(fullPath(name)) != null
  }

  implicit class LocalVariableConversion(v: Variable) {
    def asLocal: LocalVariable = v match {
      case lv: LocalVariable =>
        lv
      case _ =>
        throw new IOException("An unexpected variable was encountered for a Zookeeper operation")
    }
  }

}

/**
  * A trait that provides Apache Curator callbacks that convert to Scala promises,
  * Scala futures and eventually Java futures. The Java futures implement a trait that
  * embeds the original Scala future so that callers can access the underlying Scala future
  * without creating new threads.
  *
  * Separated from ZookeeperDirectState to its own trait to keep the callback/promise/future features
  * separate from the main State business.
  */
trait ZKBackgroundPromiseSupport { self: ZookeeperDirectState =>

  /**
    * A Callable implement that is not supposed to be called.
    */
  class Uncallable[A] extends Callable[A] {
    override def call(): A = throw new IllegalArgumentException("The call() method is unsupported")
  }

  /**
    * An inoperable Java Future. Typically used to contain a Scala Future using ScalaFutureSupport.
    */
  class UncallableJavaFuture[A] extends FutureTask[A](new Uncallable) {
    override def get(l: Long, timeUnit: java.util.concurrent.TimeUnit): A = get()
    override def get(): A = throw new IllegalArgumentException("get() is unsupported - call future() instead")
  }

  class ScalaFutureWrapper[A](val future: SFuture[A])
    extends UncallableJavaFuture[A]
    with ScalaFutureSupport[A]

  implicit class ToScalaFutureWrapper[A](val future: SFuture[A]) {
    def asScalaFutureWrapper = new ScalaFutureWrapper(future)
  }

  /**
    * Base class for our combination Curator callbacks, Scala promises, Scala future containers
    * and Java futures... all in one!
    */
  abstract class CallbackPromise[A] extends BackgroundCallback {
    val prommy: Promise[A] = Promise[A]()
    def future: SFuture[A] = prommy.future
  }

  /**
    * A callback for zookeeper.getData() which returns it wrapped in a Future.
    *
    * Uses a Promise to generate a Scala Future. Also extends Java Future, but will throw an exception if
    * called as a Java Future. This allows us to pass the Scala Future directly to Marathon/Chronos
    * via the Java-Future-based State interface.
    */
  class GetDataCallbackPromise(name: String) extends CallbackPromise[Variable] {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      val entry = StateProtos.Entry.parseFrom(event.getData)
      val lv = new LocalVariable(name, entry.getValue.toByteArray, Some(entry.getUuid.toStringUtf8))
      prommy.success(lv)
    }

    def success(v: Variable): Unit = { prommy.success(v) }
  }

  /**
    * A callback for zookeeper.setData() which then fetches the variable and returns it wrapped in a Future.
    * In other words, uses EventualVariableFetch after setData() calls this back.
    *
    * Uses a Promise to generate a Scala Future. Also extends Java Future, but will throw an exception if
    * called as a Java Future. This allows us to pass the Scala Future directly to Marathon/Chronos
    * via the Java-Future-based State interface.
    */
  class SetDataCallbackPromise(name: String) extends CallbackPromise[Variable] {

    private val eventualFetch = new GetDataCallbackPromise(name)

    override def future: SFuture[Variable] = eventualFetch.future

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      handleFetch(name, eventualFetch)
    }
  }

  /**
    * A callback for zookeeper.expunge() which returns a boolean
    *
    * Uses a Promise to generate a Scala Future. Also extends Java Future, but will throw an exception if
    * called as a Java Future. This allows us to pass the Scala Future directly to Marathon/Chronos
    * via the Java-Future-based State interface.
    */
  class ExpungeCallbackPromise extends CallbackPromise[Boolean] {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      prommy.success(true)
    }

    def success(b: Boolean): Unit = { prommy.success(b) }
  }

  /**
    * A callback for zookeeper.names() which returns an iterator of strings
    *
    * Uses a Promise to generate a Scala Future. Also extends Java Future, but will throw an exception if
    * called as a Java Future. This allows us to pass the Scala Future directly to Marathon/Chronos
    * via the Java-Future-based State interface.
    */
  class NamesCallbackPromise extends CallbackPromise[util.Iterator[String]] {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      prommy.success(event.getChildren.iterator())
    }
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

