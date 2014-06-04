package mesosphere.util

import language.postfixOps
import java.util.concurrent.{Future => JFuture, TimeUnit, ExecutionException}
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._


object BackToTheFuture {

  case class BackToTheFutureTimeout(d: Duration)

  // To use this default timeout, please "import  mesosphere.util.BackToTheFuture.Implicits._"
  object Implicits {
    implicit val default_timeout = BackToTheFutureTimeout(2 seconds)
  }

  implicit def futureToFutureOption[T](f: JFuture[T])
                                      (implicit ec: ExecutionContext, timeout: BackToTheFutureTimeout): Future[Option[T]] = {
    Future {
      try {
        Option(f.get(timeout.d.toMicros, TimeUnit.MICROSECONDS))
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
  }

  implicit def futureToFuture[T](f: JFuture[T])
                                (implicit ec: ExecutionContext, timeout: BackToTheFutureTimeout): Future[T] = {
    Future {
      try {
        f.get(timeout.d.toMicros, TimeUnit.MICROSECONDS)
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
  }

  implicit def valueToFuture[T](value: T)
                               (implicit ec: ExecutionContext, to: BackToTheFutureTimeout): Future[T] = {
    Future {
      value
    }
  }
}
