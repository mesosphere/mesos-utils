package mesosphere.util

import java.util.concurrent.{ Future => JFuture, TimeUnit, TimeoutException }

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.{ Failure, Random, Success }

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.test.TestingServer
import org.apache.curator.utils.DebugUtils
import org.apache.mesos.state.{ State, Variable, ZooKeeperState }
import org.scalatest.{ FlatSpec, Matchers }

/**
  * Tests the ZookeeperDirectState (written in Scala and using Apache Curator)
  * against the JNI-based ZooKeeperState.
  *
  * Note - the JNI-related tests require that the libmesos native library
  * be installed and pointed to by the MESOS_NATIVE_JAVA_LIBRARY env.
  */
class ZookeeperDirectStateSpec
    extends FlatSpec
    with StateTesting
    with Matchers
    with JFutureValues
    with RandomTestUtils {

  System.setProperty(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES, "false")

  lazy val zkJNIState: ZooKeeperState = createJNIState()
  lazy val zkDirectState: ZookeeperDirectState = createDirectState()

  // Run the standard suite of tests on both State implementations
  it should behave like testState(zkJNIState, "ZooKeeperState")
  it should behave like testState(zkDirectState, "ZookeeperDirectState")

  // Run tests to ensure the State implementations are fully compatible
  val compatibleZNode = znode
  val compatibleJniState = createJNIState(compatibleZNode)
  val compatibleDirectState = createDirectState(compatibleZNode)
  it should behave like interstateTesting(compatibleDirectState, compatibleJniState)
  it should behave like interstateTesting(compatibleJniState, compatibleDirectState)
  it should behave like interstateTesting(compatibleJniState, compatibleJniState)
  it should behave like interstateTesting(compatibleDirectState, compatibleDirectState)

  // Zookeeper-specific tests

  "ZookeeperDirectState" should "respond appropriately when fetch() can't reach zookeeper" in {

    val zkServer = new TestingServer(9011, true)
    val state: ZookeeperDirectState = createDirectState(serverList = zkServer.getConnectString)

    val name = randomString(20)
    val data = randomBytes(20)
    val newVar = state.fetch(name).value.mutate(data)
    state.store(newVar).value
    state.fetch(name).value.value() should equal(data)

    zkServer.stop()
    val f = state.fetch(name).toFuture

    f onComplete {
      case Success(variable)             => fail("Should not have returned a variable when zk server is down")
      case Failure(ex: TimeoutException) => println(s"Got the expected error: $ex")
      case Failure(ex)                   => fail("A TimeoutException should have been thrown")
    }

    Await.ready(f, 3.seconds)
  }

  it should "respond appropriately when store() can't reach zookeeper" in {

    val zkServer = new TestingServer(9012, true)
    val state: ZookeeperDirectState = createDirectState(serverList = zkServer.getConnectString)

    val name = randomString(20)
    val data = randomBytes(20)
    val newVar = state.fetch(name).value.mutate(data)
    state.store(newVar).value
    val retrievedVariable = state.fetch(name).value
    retrievedVariable.value() should equal(data)
    val updatedVariable = retrievedVariable.mutate("Hi There".getBytes)

    zkServer.stop()
    val f = state.store(updatedVariable).toFuture

    f onComplete {
      case Success(names)                => fail("Should not have returned names when zk server is down")
      case Failure(ex: TimeoutException) => println(s"Got the expected error: $ex")
      case Failure(ex)                   => fail("A TimeoutException should have been thrown")
    }

    Await.ready(f, 3.seconds)
  }

  it should "respond appropriately when expunge() can't reach zookeeper" in {

    val zkServer = new TestingServer(9013, true)
    val state: ZookeeperDirectState = createDirectState(serverList = zkServer.getConnectString)

    val name = randomString(20)
    val data = randomBytes(20)
    state.store(state.fetch(name).value.mutate(data)).value
    val retrievedVariable = state.fetch(name).value
    retrievedVariable.value() should equal(data)

    zkServer.stop()
    val f = state.expunge(retrievedVariable).toFuture

    f onComplete {
      case Success(names)                => fail("Should not have returned names when zk server is down")
      case Failure(ex: TimeoutException) => println(s"Got the expected error: $ex")
      case Failure(ex)                   => fail("A TimeoutException should have been thrown")
    }

    Await.ready(f, 3.seconds)
  }

  it should "respond appropriately when names() can't reach zookeeper" in {

    val zkServer = new TestingServer(9014, true)
    val state: ZookeeperDirectState = createDirectState(serverList = zkServer.getConnectString)

    zkServer.stop()
    val f = state.names.toFuture

    f onComplete {
      case Success(names)                => fail("Should not have returned names when zk server is down")
      case Failure(ex: TimeoutException) => println(s"Got the expected error: $ex")
      case Failure(ex)                   => fail("A TimeoutException should have been thrown")
    }

    Await.ready(f, 3.seconds)
  }

  def ensureZookeeperWorks(connectString: String) = {
    val c = CuratorFrameworkFactory.newClient(connectString, 2000, 2000, new RetryNTimes(0, 0))
    c.start()
    c.blockUntilConnected(2, TimeUnit.SECONDS)
    c.create().forPath("/Greetings")
    c.setData().forPath("/Greetings", "Hi".getBytes)
  }

  implicit class Unwrapping[A](j: JFuture[A]) {
    def toFuture: Future[A] = j.asInstanceOf[ScalaFutureSupport[A]].future
  }
}

trait JFutureValues {

  /**
    * In the fashion of Scalatest's OptionValues, adds a value() method to a
    * Java future to retrieve the value. Supports our ScalaFutureSupport internal
    * Scala future.
    */
  implicit class JFutureConversion[A](a: JFuture[A]) {

    val timeout = 5.seconds

    def value: A = a match {
      case f: ScalaFutureSupport[A] => Await.result[A](f.future, timeout)
      case _                        => a.get(timeout.toMillis, TimeUnit.MILLISECONDS)
    }
  }

}

/**
  * Fairly tests any implementation of State
  */
trait StateTesting extends Matchers
    with RandomTestUtils
    with ZookeeperTestServerSupport
    with JFutureValues { self: FlatSpec =>

  def znode = "/" + randomString(10)
  val timeout = 5.seconds

  // Some useful utils for creating either type of Zookeeper State
  def createJNIState(node: String = znode) = {
    new ZooKeeperState(testZKServerList, timeout.toSeconds, SECONDS, node)
  }

  def createDirectState(node: String = znode, serverList: String = testZKServerList) = {
    val mills = timeout.toMillis.toInt
    val client = CuratorFrameworkFactory.newClient(serverList, mills, mills, new RetryNTimes(0, 0))
    client.start()
    client.blockUntilConnected(5, TimeUnit.SECONDS)
    new ZookeeperDirectState(node, client, scala.concurrent.ExecutionContext.Implicits.global)
  }

  def dupe(state: State): State = state match {
    case x: ZooKeeperState       => createJNIState()
    case x: ZookeeperDirectState => createDirectState()
  }

  /**
    *  Test a generic State implementation
    */
  def testState(state: State, prefix: String): Unit = {

    def fetch(n: String): Variable = state.fetch(n).value
    def store(v: Variable): Variable = state.store(v).value
    def storeData(n: String, data: Array[Byte]): Variable = store(fetch(n).mutate(data))
    def expunge(v: Variable): Boolean = state.expunge(v).value
    def names: List[String] = state.names().value.asScala.toList

    prefix should "ensure that fetching a non-existent value will return a variable with empty data" in {
      fetch(randomString(20)).value should be (Nil)
    }

    it should "ensure that fetching an existent value will return a variable with valid data" in {
      val name = randomString(20)
      val data = randomBytes(20)
      storeData(name, data)

      fetch(name).value() should equal(data)
    }

    it should "ensure that storing an out-of-date variable will fail with a null result" in {
      val name = randomString(20)
      val data = randomBytes(20)

      val version1: Variable = storeData(name, data)
      val version2: Variable = store(fetch(name).mutate("Hi!".getBytes))

      store(version1.mutate("Hello!".getBytes)) should be(null)
    }

    it should "ensure that storing a current variable will return the new variable" in {
      val name = randomString(20)
      val data = randomBytes(20)
      val version1 = storeData(name, data)
      val version2 = store(version1.mutate("Hi!".getBytes))
      version2 should not be null
      version2.value should not be null
      new String(version2.value()) should equal("Hi!")
    }

    it should "throw an exception if asked to store a variable that is too large" in {

      val name = randomString(20)
      val data2mb = randomBytes(2 * 1024 * 1024)

      intercept[Exception] {
        storeData(name, data2mb)
      }
    }

    it should "ensure that deleting a non-existent variable will return false" in {
      expunge(fetch(randomString(20))) should equal(false)
    }

    it should "ensure that deleting an existing variable will return true" in {
      val name = randomString(20)
      val variable = storeData(name, "Yo".getBytes)
      expunge(variable) should equal(true)

      // Make sure the variable is really gone
      expunge(variable) should equal(false)
      fetch(name).value().size should equal(0)
      fetch(name).value().size should equal(0)
    }

    it should "ensure that names() will include stored entries" in {
      val name = randomString(20)
      val variable = storeData(name, "Hi There".getBytes)
      names should contain(name)
    }

    it should "ensure that names() will not include non-existent entries" in {
      val name = randomString(20)
      names should not contain name
    }

    it should "ensure that names() will not include fetched-but-not-stored entries" in {
      val name = randomString(20)
      fetch(name)
      names should not contain name
    }

    it should "support fetch() when the znode doesn't exist" in {
      val s = dupe(state)
      s.fetch("blah").value should not be (null)
    }

    it should "support store() when the znode doesn't exist" in {
      val s = dupe(state)
      s.store(s.fetch("howdy").value.mutate("Yo".getBytes)).value should not be (null)
    }

  }

  /**
    * Test two State implementations
    */
  def interstateTesting(src: State, dest: State): Unit = {

    s"${dest.getClass.getSimpleName}" should s"be able to read data stored by ${src.getClass.getSimpleName}" in {
      val name = randomString(20)
      val data = randomBytes(20)

      val variable = src.fetch(name).value
      val srcVariable = src.store(variable.mutate(data)).value

      // The dest State should be able to read data saved by the src State
      val destVariable = dest.fetch(name).value
      destVariable.value() should equal(data)
    }

  }

}

object ZookeeperTestServer {
  lazy val server = new TestingServer()
}

trait ZookeeperTestServerSupport {
  lazy val testZKServerList: String = ZookeeperTestServer.server.getConnectString
}

trait RandomTestUtils {

  def randomString(size: Int) = new String(Random.alphanumeric.take(size).toArray)

  def randomBytes(size: Int): Array[Byte] = {
    val b = new Array[Byte](size)
    Random.nextBytes(b)
    b
  }
}

