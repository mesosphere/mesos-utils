package mesosphere.util

import language.postfixOps
import java.util.concurrent.{ Future => JFuture, TimeUnit, ExecutionException }
import scala.concurrent.{ Future, ExecutionContext, blocking }
import scala.concurrent.duration._
import scala.language.implicitConversions

object BackToTheFuture {

  case class Timeout(duration: Duration)

  // To use this default timeout, please "import  mesosphere.util.BackToTheFuture.Implicits._"
  object Implicits {
    implicit val defaultTimeout = Timeout(2 seconds)
  }

  implicit def futureToFutureOption[T](f: JFuture[T])(implicit ec: ExecutionContext, timeout: Timeout): Future[Option[T]] = {
    Future {
      blocking {
        try {
          Option(f.get(timeout.duration.toMicros, TimeUnit.MICROSECONDS))
        }
        catch {
          case e: ExecutionException => throw e.getCause
        }
      }
    }
  }

  implicit def futureToFuture[T](f: JFuture[T])(implicit ec: ExecutionContext, timeout: Timeout): Future[T] = {
    Future {
      blocking {
        try {
          f.get(timeout.duration.toMicros, TimeUnit.MICROSECONDS)
        }
        catch {
          case e: ExecutionException => throw e.getCause
        }
      }
    }
  }

  implicit def valueToFuture[T](value: T)(implicit ec: ExecutionContext, timeout: Timeout): Future[T] = {
    Future {
      value
    }
  }
}
