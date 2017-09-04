package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120

    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
    @transient lazy val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.parallelize(
      List("1,10,,,2,C++",
        "2,11,,10,4,",
        "2,12,,10,1,",
        "1,13,,,3,PHP",
        "2,14,,13,3,",
        "1,15,,,0,Python"))

    val raw: RDD[Posting]                                  = rawPostings(lines)
    val grouped: RDD[(Int, Iterable[(Posting, Posting)])]  = groupedPostings(raw)
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  override def afterAll(): Unit = {
    import testObject._
    sc.stop()
  }

  test("'rawPostings' should work.") {
    val raw = testObject.raw
    val res1 = raw.count() == 6
    val res2 = raw.first().id == 10
    assert(res1)
    assert(res2)
  }

  test("'groupedPostings' should work.") {
    val grouped = testObject.grouped
    val totalGroupNumIsTwo = grouped.count() == 2
    val groupSorted: RDD[(QID, Iterable[(Answer, Answer)])] = grouped.sortBy((tup: (Int, Iterable[(Posting, Posting)])) => tup._1)
    val qid10Has2Ans = groupSorted.take(2)(0)._2.size == 2
    val qid13Has1Ans = groupSorted.take(2)(1)._2.size == 1
    assert(totalGroupNumIsTwo)
    assert(qid10Has2Ans)
    assert(qid13Has1Ans)
  }

}
