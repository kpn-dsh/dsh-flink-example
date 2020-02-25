package dsh.flink.example

import com.codahale.metrics.{Meter => CodaMeter}
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 *
 */
class BlackHole[T] extends RichSinkFunction[T] with LazyLogging {
  @transient private var meter: Meter = _

  override def open(parameters: Configuration): Unit = {
    meter = getRuntimeContext.getMetricGroup.meter("blackhole.swallowed", new DropwizardMeterWrapper(new CodaMeter))
  }

  override def close(): Unit = ()

  override def invoke(element: T, ctx: SinkFunction.Context[_]): Unit = {
    logger.info(s"swallow element: $element - rate:${meter.getRate.toLong} msgs/s}")
    meter.markEvent()
  }
}
