package mesosphere.util

import java.util.logging.{Logger, Level}
import scala.concurrent._
import scala.concurrent.duration.Duration

/**
 * Author: Shingo Omura
 */
object RetriableFuture {

  trait Retry {
    val _log = Logger.getLogger(getClass.getName)

    /**
     * protected generic retry function.  This supports variable trials.
     * User can decorate fnc over trials (e.g. exponential backoffs).
     *
     * @param count    retry count remaining (0 means only 1 try.)
     * @param task     () => Future[T]
     * @param nextTry  generator for next try whose input is the number of remaining trials
     * @tparam T       output type
     * @return         Future[T]
     */
    protected def retry[T](count: Int)
                          (promise: () => Future[T])
                          (nextTry: Int => (() => Future[T]))
                          (implicit ec: ExecutionContext): Future[T] = {
      promise() recoverWith {
        case t =>
          _log.log(Level.WARNING, "Exception caught:", t)
          if (count < 1) {
            _log.log(Level.SEVERE, "Giving up retrying.")
            throw t
          }
          else {
            _log.log(Level.WARNING, "Retrying %d more times.".format(count - 1))
            retry(count - 1)(nextTry(count - 1))(nextTry)
          }
      }
    }
  }


  /**
   * The basic retry with counting.
   */
  object CountingRetry extends Retry {

    /**
     * basic retriable future
     *
     * @param count  retry count remaining
     * @param block  usual function to be executed
     * @param ec     execution context
     * @tparam T     output type
     * @return       Future[O]
     */
    def apply[T](count: Int)(promise: () => Future[T])
                (implicit ec: ExecutionContext) = {
      retry(count)(promise)(_ => promise)
    }
  }


  /**
   * Retry with exponential backoff
   */
  object ExponentialBackOffRetry extends Retry {
    protected def sleep(delay: Duration)(implicit ec: ExecutionContext): Future[Unit] =
      future {
        _log.log(Level.WARNING, "waiting backoff duration: %s".format(delay))
        Thread.sleep(delay.toMillis)
      }

    /**
     * @param max_try number of retry
     * @param delay   delay
     * @param factor  factor of exponential backoff
     * @param block   block to be executed
     * @param ec      execution context
     * @tparam T      type of output
     * @return        Future[O]
     */
    def apply[T](max_try: Int, delay: Duration, factor: Int)
                (promise: () => Future[T])
                (implicit ec: ExecutionContext): Future[T] = {
      retry(max_try)(promise) {
        remaining: Int =>
          () => sleep(delay * scala.math.pow(factor, max_try - 1 - remaining)).flatMap(_ => promise())
      }
    }
  }

}
