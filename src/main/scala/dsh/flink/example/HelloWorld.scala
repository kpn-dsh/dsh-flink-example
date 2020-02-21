package dsh.flink.example

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.codahale.metrics.{ExponentiallyDecayingReservoir, Histogram => CodaHist, Meter => CodaMeter}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.{DropwizardHistogramWrapper, DropwizardMeterWrapper}
import org.apache.flink.metrics.{Histogram, Meter}
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import java.util.{List => JList}
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.util.Try

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    env.enableCheckpointing(1000)

    lazy val kafkaSink = new FlinkKafkaProducer011[Long](
      "scratch.flink.dshtest",
      new SerializationSchema[Long]() {
        override def serialize(element: Long): Array[Byte] = ("PAYLOAD TIMESTAMP: " + element).getBytes(StandardCharsets.UTF_8)
      },
      ConfigMgr.producerConfig)

    lazy val kafkaSource = new FlinkKafkaConsumer011[String](
      "scratch.flink.dshtest",
      new DeserializationSchema[String]() {
        override def deserialize(message: Array[Byte]): String = new String(message)
        override def isEndOfStream(nextElement: String): Boolean = false
        override def getProducedType: TypeInformation[String] = createTypeInformation[String]
      },
      ConfigMgr.consumerConfig)

    // stream -- 1
    lazy val sink = ConfigMgr.onValidConfig(ConfigMgr.producerConfig)(kafkaSink).getOrElse(new BlackHole)

    env
      .addSource(new ClockSource).name("Clock")
      .map(tick => tick.ts).name("extract tick")
      .addSink(sink).name("ToKafka (or void)")

    // stream -- 2
    ConfigMgr.onValidConfig(ConfigMgr.consumerConfig) {
      env
        .addSource(kafkaSource).name("FromKafka")
        .map(str => str.split(':').last).name("extract clock")
        .map(nr => Try(nr.toLong).getOrElse(-1L))
        .addSink(new BlackHole).name("Swallow")
    }

    env.execute("FLINK -- Hello World")
  }
}

class BlackHole extends RichSinkFunction[Long] {
  @transient private var meter: Meter = _
  @transient private var histo: Histogram = _
  @transient private var scheduler: ScheduledThreadPoolExecutor = _

  override def open(parameters: Configuration): Unit = {
    meter = getRuntimeContext.getMetricGroup.meter("blackhole.swallowed", new DropwizardMeterWrapper(new CodaMeter))
    histo = getRuntimeContext.getMetricGroup.histogram("blackhole.latency", new DropwizardHistogramWrapper(new CodaHist(new ExponentiallyDecayingReservoir())))
    scheduler = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      override def run(): Unit = {
        println(s"[${getRuntimeContext.getTaskName}}] happily swallowing ticks -- already ate ${meter.getCount} of them ... (${meter.getRate.toLong} msgs/s)")
      }
    }
    scheduler.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)
  }

  override def close(): Unit = Option(scheduler).foreach(_.shutdown())

  override def invoke(value: Long, ctx: SinkFunction.Context[_]): Unit = {
    meter.markEvent()
    histo.update(ctx.currentProcessingTime() - value)
  }
}

case class Tick(ts: Long)
class ClockSource extends RichParallelSourceFunction[Tick] with ListCheckpointed[java.lang.Long] with LazyLogging {
  private var running = false
  @transient private var meter: Meter = _
  @transient private var scheduler: ScheduledThreadPoolExecutor = _
  @transient private var offset = 0L

  override def open(parameters: Configuration): Unit = {
    meter = getRuntimeContext.getMetricGroup.meter("clock.ticks", new DropwizardMeterWrapper(new CodaMeter))
    scheduler = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      override def run(): Unit = println(s"[${getRuntimeContext.getTaskName}}] happily spawning ticks -- already generated ${meter.getCount} of them ... (${meter.getRate.toLong} msgs/s)")
    }

    scheduler.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)
    running = true
  }

  override def close(): Unit = Option(scheduler).foreach(_.shutdown())

  override def run(ctx: SourceFunction.SourceContext[Tick]): Unit = {
    val lock = ctx.getCheckpointLock
    @tailrec def runner(block: => Unit): Unit = if (running) { lock.synchronized(block); runner(block) }

    logger.info(s"STARTING CLOCK SOURCE @ OFFSET: $offset")

    runner {
      ctx.collect(Tick(offset))
      offset += 1
      meter.markEvent()
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