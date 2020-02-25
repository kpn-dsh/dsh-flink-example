package dsh.flink.example

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.CommonClientConfigs

/**
 *
 */
object HelloWorld extends LazyLogging {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.enableCheckpointing(1000)

    lazy val sink: RichSinkFunction[(String, Clock.Tick)] = Option(ConfigMgr.producerConfig)
      .filter(_.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
      .map(KafkaUtils.flinkProducer)
      .getOrElse(new BlackHole[(String, Clock.Tick)])

    lazy val sourceFromKafkaMaybe = Option(ConfigMgr.consumerConfig)
      .filter(_.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
      .map(KafkaUtils.flinkConsumer)

    // stream -- 1
    env
      .addSource(new Clock.Source).name("Clock")
      .map(tick => ConfigMgr.key -> tick)
      .addSink(sink).name("ToKafka (or void)")

    // stream -- 2
    sourceFromKafkaMaybe.foreach(kafkaSource =>
      env
        .addSource(kafkaSource).name("FromKafka")
        .map { tick =>
          logger.info(s"got a tick: ${tick}")
          tick
        }
        .filter(_ != null).name("filter out non clock ticks")
        .addSink(new BlackHole[Clock.Tick]).name("Swallow"))

    env.execute("FLINK -- Hello World")
  }
}
