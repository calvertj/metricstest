package com.nibr

import java.io.{OutputStreamWriter, PrintWriter}
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
import org.apache.spark.rdd.MetricsContext._
import java.io._

import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener, Metrics}

/**
Use the Force go to the source
  */
class MetricsTest extends LocalSparkTest {
  "metrics" should "run with various spark version" in {
    Metrics.initialize(sc)
    val metricsListener = new MetricsListener(new RecordedMetrics())
    sc.addSparkListener(metricsListener)

    val col = sc.parallelize(0 to 100 by 5).instrument()
    val smp = col.sample(true, 4)
    val colCount = col.count
    val smpCount = smp.count

    val writer = new PrintWriter(new OutputStreamWriter(System.out))
    Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    writer.close()
  }
}
