package mesosphere.util

import java.io.IOException
import java.lang.Boolean
import java.util
import java.util.UUID
import java.util.concurrent.{ Callable, Future => JFuture, FutureTask, TimeoutException }

import scala.concurrent.{ ExecutionContext, Future => SFuture, Promise }

import com.google.protobuf.ByteString
import mesosphere.mesos.protos.StateProtos
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api._
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
  * The State methods return a subtype of Java's Future that implements ScalaFutureSupport
  * and contains a Scala Future which may eventually contain the expected value.
  */

trait ScalaFutureSupport[A] {
  def future: SFuture[A]
}

class ZookeeperDirectState(zNode: String, client: CuratorFramework, implicit val context: ExecutionContext)
    extends State with ZKBackgroundPromiseSupport {

  private val MAX_ZK_SIZE_IN_BYTES = 0xfffff

  /**
    * Fetches the data from ZK.
    *
    * Cast this to ScalaFutureSupport to access the valid Scala future, as the Java future is inoperative.
    */
  override def fetch(name: String): JFuture[Variable] = {
    val result: SFuture[Variable] = connectionFailedFuture getOrElse handleFetch(name)
    result.asScalaFutureWrapper
  }

  /**
    * Marshall a variable to an Entry and store it in ZK.
    *
    * Cast this to ScalaFutureSupport to access the valid Scala future, as the Java future is inoperative.
    */
  override def store(variable: Variable): JFuture[Variable] = {

    if (variable.value().size > MAX_ZK_SIZE_IN_BYTES) {
      throw new IOException("Serialized data is too big (> 1 MB)")
    }

    val result: SFuture[Variable] = connectionFailedFuture getOrElse handleStore(variable.asLocal)
    result.asScalaFutureWrapper
  }

  /**
    * Remove the variable from ZK. Returns false if deletion was not necessary.
    *
    * Cast this to ScalaFutureSupport to access the valid Scala future, as the Java future is inoperative.
    */
  override def expunge(variable: Variable): JFuture[Boolean] = {
    val result: SFuture[Boolean] = connectionFailedFuture getOrElse handleExpunge(variable.asLocal)
    result.asScalaFutureWrapper
  }

  /**
    * Returns the children of our ZK node.
    *
    * Cast this to ScalaFutureSupport to access the valid Scala future, as the Java future is inoperative.
    */
  override def names: JFuture[util.Iterator[String]] = {
    val result: SFuture[util.Iterator[String]] = connectionFailedFuture getOrElse handleNames
    result.asScalaFutureWrapper
  }

  /**
    * If Zookeeper is connected returns None, otherwise returns a failed future
    * with a TimeoutException.
    */
  private def connectionFailedFuture[A]: Option[SFuture[A]] = {
    client.getZookeeperClient.isConnected match {
      case true  => None
      case false => Some(SFuture.failed[A](new TimeoutException("Could not connect to Zookeeper")))
    }
  }

  /**
    * Fetches the data from ZK. Returns an instance of ScalaFutureSupport type containing a Scala-Future.
    * Uses a given callback to convert Curator's result to a Promise and then a Scala Future.
    */
  private def handleFetch(name: String): SFuture[LocalVariable] = {

    if (name == null) {
      throw new RuntimeException("Whoah, name is null!")
    }

    exists(name) flatMap {
      case x if x  => handleFetchForExistingPath(name)
      case x if !x => SFuture.successful(new LocalVariable(name))
    }
  }

  private def handleFetchForExistingPath(name: String): SFuture[LocalVariable] = {
    val callback = new GetDataCallbackPromise(name)
    client.getData.inBackground(callback).forPath(fullPath(name))
    callback.future
  }

  /**
    * Marshall a variable to an Entry and store it in ZK.
    *
    * First fetches a variable and then flat maps its Scala future
    * as the second argument to store(LocalVariable, LocalVariable)
    * so it can ensure the right version is being updated.
    * Compares the variable with the latest persisted one to prevent an obsolete
    * variable from being persisted.
    *
    * Returns a future with the variable if the versions match, else a future with a null
    */
  private def handleStore(lv: LocalVariable): SFuture[LocalVariable] = {

    def doStore(pv: LocalVariable) = {
      val callback = new SetDataCallbackPromise(lv.name)
      val entryData = marshall(lv)
      client.setData().inBackground(callback).forPath(fullPath(lv.name), entryData)
      callback.future.map(_ => parseLocalVariable(lv.name, entryData))
    }

    for {
      pv <- handleFetch(lv.name)
      pathsCreated <- mkdirCompletePath(fullPath(lv.name))
      versionsMatch = lv.storedUuid == pv.storedUuid
      result <- if (versionsMatch) doStore(pv) else SFuture.successful(null)
    } yield result
  }

  /**
    * Convert a local variable into an Entry and then marshall it to a byte array
    */
  private def marshall(v: LocalVariable): Array[Byte] = {
    StateProtos.Entry.newBuilder()
      .setName(v.name)
      .setUuid(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      .setValue(ByteString.copyFrom(v.value()))
      .build()
      .toByteArray
  }

  /**
    * Remove the variable from ZK. Returns false if deletion was not necessary.
    * Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  private def handleExpunge(lv: LocalVariable): SFuture[Boolean] = {
    exists(lv.name) flatMap {
      case x if x  => handleExpungeForExistingPath(lv)
      case x if !x => SFuture.successful(false)
    }
  }

  private def handleExpungeForExistingPath(lv: LocalVariable): SFuture[Boolean] = {
    val callback = new ExpungeCallbackPromise()
    client.delete.inBackground(callback).forPath(fullPath(lv.name))
    callback.future
  }

  /**
    * Returns the children of our ZK node.
    * Returns an instance of ScalaFutureSupport type containing a Scala-Future
    */
  private def handleNames: SFuture[util.Iterator[String]] = {
    val callback = new NamesCallbackPromise
    client.getChildren.inBackground(callback).forPath(zNode)
    callback.future
  }

  private def fullPath(name: String) = s"$zNode/$name"

  private def mkDir(name: String): Unit = {
    val path = fullPath(name)
    ZKPaths.mkdirs(client.getZookeeperClient.getZooKeeper, path)
  }

  private def mkdirCompletePath(path: String): SFuture[Boolean] = SFuture {
    ZKPaths.mkdirs(client.getZookeeperClient.getZooKeeper, path)
    true
  }

  // Disabled - ZooKeeper is taking these async callbacks out of order, failing them.
  // - If there is a way to fix that, we should investigate a good way to do this
  //
  //  /**
  //    * Creates empty ZK nodes for each item in slash-delimited path.
  //    * TODO optimize by reverse-searching paths to prevent unnecessary checks
  //    */
  //  private def mkdirCompletePath(path: String): SFuture[Seq[Boolean]] = {
  //
  //    def mkNode(path: String): SFuture[Boolean] = {
  //      println(s"Making node $path")
  //      val callback = new CreateCallbackPromise
  //      client.create().inBackground(callback).forPath(path)
  //      callback.future
  //    }
  //
  //    def mkNodeIfNotExists(path: String): SFuture[Boolean] = {
  //      for {
  //        pathExists <- existsFullPath(path)
  //        if !pathExists
  //        nodeFuture <- mkNode(path)
  //      } yield nodeFuture
  //    }
  //
  //    val parts = path split "/" filter (_.nonEmpty)
  //
  //    val nodeMakingFutures: Seq[SFuture[Boolean]] = for {
  //      i <- 1 to parts.size
  //      subPathItems = parts take i
  //      subPath = subPathItems.mkString("/", "/", "")
  //      nodeFuture = mkNodeIfNotExists(subPath)
  //    } yield nodeFuture
  //
  //    SFuture.sequence(nodeMakingFutures)
  //  }

  private def exists(name: String): SFuture[Boolean] = existsFullPath(fullPath(name))

  private def existsFullPath(name: String): SFuture[Boolean] = {
    val callback = new ExistsCallbackPromise
    client.checkExists.inBackground(callback).forPath(name)
    callback.future
  }

  def parseLocalVariable(name: String, entryData: Array[Byte]): LocalVariable = {
    val entry = StateProtos.Entry.parseFrom(entryData)
    new LocalVariable(name, entry.getValue.toByteArray, Some(entry.getUuid.toStringUtf8))
  }

  /**
    * Assists with conversion from the Variable interface to our own implementation, LocalVariable
    */
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
    protected val promise: Promise[A] = Promise[A]()
    def future: SFuture[A] = promise.future
  }

  /**
    * A callback for zookeeper.getData() which returns it wrapped in a Future.
    *
    * Uses a Promise to generate a Scala Future. Also extends Java Future, but will throw an exception if
    * called as a Java Future. This allows us to pass the Scala Future directly to Marathon/Chronos
    * via the Java-Future-based State interface.
    */
  class GetDataCallbackPromise(name: String) extends CallbackPromise[LocalVariable] {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      val lv = parseLocalVariable(name, event.getData)
      promise.success(lv)
    }
  }

  /**
    * A callback for zookeeper.setData() which then fetches the variable and returns it wrapped in a Future.
    * In other words, uses EventualVariableFetch after setData() calls this back.
    *
    * Uses a Promise to generate a Scala Future. Also extends Java Future, but will throw an exception if
    * called as a Java Future. This allows us to pass the Scala Future directly to Marathon/Chronos
    * via the Java-Future-based State interface.
    */
  class SetDataCallbackPromise(name: String) extends CallbackPromise[Boolean] {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      promise.success(true)
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
      promise.success(true)
    }

    def success(b: Boolean): Unit = promise.success(b)
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
      promise.success(event.getChildren.iterator())
    }
  }

  /**
    * A callback for zookeeper.create()
    */
  class CreateCallbackPromise extends CallbackPromise[Boolean] {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      promise.success(true)
    }
  }

  /**
    * A callback for zookeeper.exists() which returns true if the item exists
    *
    * Uses a Promise to generate a Scala Future. Also extends Java Future, but will throw an exception if
    * called as a Java Future. This allows us to pass the Scala Future directly to Marathon/Chronos
    * via the Java-Future-based State interface.
    */
  class ExistsCallbackPromise extends CallbackPromise[Boolean] {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      val exists = event.getStat != null
      promise.success(exists)
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

