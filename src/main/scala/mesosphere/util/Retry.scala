package mesosphere.util

import scala.util.Try
import scala.concurrent.duration.Duration

/**
 * @author Florian Leibert
 */

object Retry {

  /**
   * A general retry combinator with exponential back-off and a `try-catch`
   * flavor.
   *
   * {{{
   * retry(2, 2.seconds) {
   *   throw new java.lang.IllegalStateException
   * } {
   *  catch e: java.lang.IllegalStateException =>
   *    System.err.println("Illegal state encountered.")
   * }
   * }}}
   *
   * @param tries   Times to retry. 0 means the block is evaluated once.
   * @param wait    Time to wait between retries.
   * @param fnc     Block to evaluate.
   * @param handler Handler block for exceptions.
   * @tparam T      Result type.
   * @return        Result of evaluation of `fnc`, if successful.
   */
  def retry[T](tries: Int, wait: Duration)
              (fnc: => T)
              (handler: PartialFunction[Throwable, Any]): T = {
    for (n <- 1 to tries) {
      // Note that Try catches only non-fatal exceptions.
      val attempt: Try[T] = Try(fnc)
      if (attempt.isSuccess) return attempt.get
      attempt.recover(handler).get // Throws if handler can't handle it.
      Thread.sleep(n * wait.toMillis)
    }
    fnc // Our last, best hope.
  }
}
