package dsh.flink.example

import com.codahale.metrics.{ExponentiallyDecayingReservoir, Histogram => CodaHist, Meter => CodaMeter}
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.{DropwizardHistogramWrapper, DropwizardMeterWrapper}
import org.apache.flink.metrics.{Histogram, Meter}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import scala.annotation.tailrec

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val stream = env.addSource(new ClockSource)
    stream
      .map(tick => tick.ts)
      .addSink(new BlackHole)

    env.execute("FLINK -- Hello World")
  }
}

class BlackHole extends RichSinkFunction[Long] {
  @transient private var meter: Meter = _
  @transient private var histo: Histogram = _

  override def open(parameters: Configuration): Unit = {
    meter = getRuntimeContext.getMetricGroup.meter("blackhole.swallowed", new DropwizardMeterWrapper(new CodaMeter))
    histo = getRuntimeContext.getMetricGroup.histogram("blackhole.latency", new DropwizardHistogramWrapper(new CodaHist(new ExponentiallyDecayingReservoir())))
  }

  override def close(): Unit = ()

  override def invoke(value: Long, ctx: SinkFunction.Context[_]): Unit = {
    meter.markEvent()
    histo.update(ctx.currentProcessingTime() - value)
  }
}

case class Tick(ts: Long)
class ClockSource extends RichParallelSourceFunction[Tick] {
  private var running = false
  @transient private var meter: Meter = _

  override def open(parameters: Configuration): Unit = {
    meter = getRuntimeContext.getMetricGroup.meter("clock.ticks", new DropwizardMeterWrapper(new CodaMeter))
    running = true
  }

  override def run(ctx: SourceFunction.SourceContext[Tick]): Unit = {
    @tailrec def whileRunning(block: => Unit): Unit = if (running) { block; whileRunning(block) }

    whileRunning {
      ctx.collect(Tick(System.currentTimeMillis()))
      meter.markEvent()
    }
  }

  override def cancel(): Unit = running = false
}