package mesosphere.util

import java.util.concurrent.ExecutionException
import java.util.concurrent.{Future => JFuture}
import scala.concurrent.{Future, ExecutionContext}


object BackToTheFuture {


  implicit def futureToFutureOption[T](f: JFuture[T])(implicit ec: ExecutionContext): Future[Option[T]] = {
    Future {
      try {
        Option(f.get)
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
  }

  implicit def futureToFuture[T](f: JFuture[T])(implicit ec: ExecutionContext): Future[T] = {
    Future {
      try {
        f.get
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
  }

  implicit def valueToFuture[T](value: T)(implicit ec: ExecutionContext): Future[T] = {
    Future {
      value
    }
  }
}
