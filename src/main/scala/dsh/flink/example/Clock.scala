package dsh.flink.example

import java.util.{List => JList}

import com.codahale.metrics.{Meter => CodaMeter}
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 *
 */
object Clock {
  case class Tick(ts: Long)

  class Source extends RichParallelSourceFunction[Tick] with ListCheckpointed[java.lang.Long] with LazyLogging {
    private var running = false
    @transient private var meter: Meter = _
    @transient private var offset = 0L

    override def open(parameters: Configuration): Unit = {
      meter = getRuntimeContext.getMetricGroup.meter("clock.ticks", new DropwizardMeterWrapper(new CodaMeter))
      running = true
    }

    override def close(): Unit = ()

    override def run(ctx: SourceFunction.SourceContext[Tick]): Unit = {
      val lock = ctx.getCheckpointLock
      @tailrec def runner(block: => Unit): Unit = if (running) { lock.synchronized(block); runner(block) }

      logger.info(s"STARTING CLOCK SOURCE @ OFFSET: ${meter.getCount}")

      runner {
        logger.info(s"submit clock tick - offset:$offset, rate: ${meter.getRate.toLong} msgs/s}")
        ctx.collect(Tick(offset))
        meter.markEvent(); offset += 1
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = running = false

    override def snapshotState(checkpointId: Long, timestamp: Long): JList[java.lang.Long] = {
      logger.info(s"SNAPSHOTTING - CLOCK SOURCE @ OFFSET: $offset, id: $checkpointId")
      List(Long.box(offset)).asJava
    }

    override def restoreState(state: JList[java.lang.Long]): Unit = {
      logger.info(s"RESTORING SNAPSHOT - CLOCK SOURCE @ OFFSET: $offset -- STATE: ${state.asScala.mkString(",")}")
      state.asScala.foreach(offset = _)
    }
  }
}
