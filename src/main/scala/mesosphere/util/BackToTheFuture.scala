package mesosphere.util

import scala.concurrent._
import scala.Some
import java.util.concurrent.ExecutionException


object BackToTheFuture {

  import ExecutionContext.Implicits.global

  implicit def FutureToFutureOption[T](f: java.util.concurrent.Future[T]): Future[Option[T]] = {
    future {
      try {
        Option(f.get)
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
  }

  implicit def FutureToFuture[T](f: java.util.concurrent.Future[T]): Future[T] = {
    future {
      try {
        f.get
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
  }

  implicit def ValueToFuture[T](value: T): Future[T] = {
    future {
      value
    }
  }
}
