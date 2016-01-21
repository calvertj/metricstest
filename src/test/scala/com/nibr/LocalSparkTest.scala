package com.nibr

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

abstract class TestBase extends FlatSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
  override def beforeAll() {}
  override def afterAll() {}
}


/** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
trait LocalSparkTest extends TestBase with Suite {
  self: Suite =>

  var sc: SparkContext = null
  def getSparkContext: SparkContext = {

    val sparkConfiguration: SparkConf = LocalSparkTest.getSparkConf()
    val masterNode: String = "local"
    new SparkContext(masterNode, "test", sparkConfiguration)
  }

  override def beforeAll() {
    super.beforeAll()
  }

  override def beforeEach() {
    resetSparkContext()
    sc = getSparkContext
    super.beforeEach()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() = {
    LocalSparkTest.stop(sc)
    sc = null
  }

}

object LocalSparkTest {

  def getSparkConf() = new SparkConf()

  def withSpark[T](sc: SparkContext)(f: SparkContext => T) = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

  def stop(sc: SparkContext) {
    if (sc != null) {
      sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

}

