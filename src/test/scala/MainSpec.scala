import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite
import utils.SparkTestSession

class MainSpec extends AnyFunSuite with SparkTestSession {
  val testLogger: Logger    = Logger.getLogger("TestLogger")
  lazy val sc               = spark.sparkContext
  implicit val sparkSession = spark
  implicit val logger       = testLogger

  test("test demo") {
    assert(true)
  }
}
