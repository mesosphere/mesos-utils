package mesosphere.util

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import scala.util.Random

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.mesos.state.{ Variable, State, ZooKeeperState }
import scala.collection.JavaConverters._

import java.util.concurrent.{ Future => JFuture, TimeUnit }

import org.scalatest.{ FlatSpec, Matchers }

class ZookeeperDirectStateSpec extends FlatSpec with StateTesting
    with ZookeeperTestServerSupport
    with RandomTestUtils {

  val znode = "/" + randomString(10)
  val timeout = 5.seconds

  lazy val zkJNIState: ZooKeeperState = new ZooKeeperState(
    testZKServerList,
    timeout.toSeconds,
    SECONDS,
    znode
  )

  lazy val zkDirectState: ZookeeperDirectState = {

    val client = CuratorFrameworkFactory.newClient(
      testZKServerList,
      2000,
      2000,
      new ExponentialBackoffRetry(1000, 4)
    )

    client.start()

    new ZookeeperDirectState(znode, client, scala.concurrent.ExecutionContext.Implicits.global)
  }

  // Note - the JNI-related tests require that the libmesos native library
  // be installed and pointed to by the MESOS_NATIVE_JAVA_LIBRARY env.

  // Run the standard suite of tests on both State implementations
  it should behave like testState(zkJNIState, "ZooKeeperState")
  it should behave like testState(zkDirectState, "ZookeeperDirectState")

  // Run tests to ensure the State implementations are fully compatible
  it should behave like interstateTesting(zkDirectState, zkJNIState)
  it should behave like interstateTesting(zkJNIState, zkDirectState)
  it should behave like interstateTesting(zkJNIState, zkJNIState)
  it should behave like interstateTesting(zkDirectState, zkDirectState)

}

trait JFutureValues {

  /**
    * In the fashion of Scalatest's OptionValues, adds a value() method to a
    * Java future to retrieve the value. Supports our ScalaFutureSupport internal
    * Scala future.
    */
  implicit class JFutureConversion[A](a: JFuture[A]) {

    val timeout = 10.seconds

    def value: A = a match {
      case f: ScalaFutureSupport[A] => Await.result[A](f.future, timeout)
      case _                        => a.get(timeout.toMillis, TimeUnit.MILLISECONDS)
    }
  }

}

/**
  * Fairly tests any implementation of State
  */
trait StateTesting extends Matchers with RandomTestUtils with JFutureValues { self: FlatSpec =>

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

    it should "ensure that names will include stored entries" in {
      val name = randomString(20)
      val variable = storeData(name, "Hi There".getBytes)
      names should contain(name)
    }

    it should "ensure that names will not include non-existent entries" in {
      val name = randomString(20)
      names should not contain name
    }

    it should "ensure that names will not include fetched-but-not-stored entries" in {
      val name = randomString(20)
      fetch(name)
      names should not contain name
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
  val server = new TestingServer()
}

trait ZookeeperTestServerSupport {
  val testZKServerList: String = ZookeeperTestServer.server.getConnectString
}

trait RandomTestUtils {

  def randomString(size: Int) = new String(Random.alphanumeric.take(size).toArray)

  def randomBytes(size: Int): Array[Byte] = {
    val b = new Array[Byte](size)
    Random.nextBytes(b)
    b
  }
}