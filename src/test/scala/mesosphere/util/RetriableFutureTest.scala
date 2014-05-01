package mesosphere.util

import language.postfixOps
import org.scalatest.FlatSpec
import scala.concurrent._
import scala.concurrent.duration._

/**
 * Author: Shingo Omura.
 */
class RetriableFutureTest extends FlatSpec {

  import scala.concurrent.ExecutionContext.Implicits.global
  import mesosphere.util.RetriableFuture._

  it should "CountingRetry(2)(incrementAndFail)(1) increment 1 for three 3 times(1 + 2 re-tries)." in {
    var i = 0

    val f = CountingRetry(2){ () =>
      future {
        i += 1
        throw new Exception()
      }
    }

    try {
      Await.result(f, 2 second)
    } catch {
      case t: Throwable =>
        assert(t.isInstanceOf[Exception])
        assert(i == 3)
    }
  }

  it should "ExponentialBackOffRetry(3, 100 milli, 2)(incrementAndFail)(1) increment 1 for 4 times and it takes at least 700 millis.(100 + 200 + 400)" in {
    var i = 0

    val start = System.currentTimeMillis()
    var time = 0L
    // 1stTry --> 1sec --> ReTry --> 2 sec --> ReTry --> 4sec --> ReTry.
    val f = ExponentialBackOffRetry(3, 100 millis, 2){() =>
      future {
        i += 1
        throw new Exception()
      }
    }
    f.onFailure{
      case t: Throwable =>
        val now = System.currentTimeMillis()
        time = now - start
    }

    try {
      Await.result(f, 100 second)
    } catch {
      case t: Throwable =>
        assert(t.isInstanceOf[Exception])
        assert(time >= 700)
    }
  }
}
