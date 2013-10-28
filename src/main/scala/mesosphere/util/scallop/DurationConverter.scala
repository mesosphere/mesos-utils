package mesosphere.util.scallop

import org.rogach.scallop._
import scala.concurrent.duration._
import scala.reflect.runtime.universe._


class DurationConverter {
  implicit val durationConverter = new ValueConverter[Duration] {
    def parse(s: List[(String, List[String])]) = {
      s match {
        case (_, string :: Nil) :: Nil => {
          val UnitHMS = "([0-9]+)([hms])".r
          string match {
            case UnitHMS(num, "h") => Right(Some(num.toInt.hours))
            case UnitHMS(num, "m") => Right(Some(num.toInt.minutes))
            case UnitHMS(num, "s") => Right(Some(num.toInt.seconds))
            case _ => Left()
          }
        }
        case Nil => Right(None)
        case _ => Left()
      }
    }
    val tag = typeTag[Duration]
    val argType = ArgType.SINGLE
  }
}