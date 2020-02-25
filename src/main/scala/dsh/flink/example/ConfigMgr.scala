package dsh.flink.example

import java.util.Properties

import com.typesafe.config.ConfigFactory
import dsh.kafka.KafkaParser
import dsh.kafka.KafkaParser.ConsumerGroupType
import dsh.messages.Envelope
import dsh.sdk.Sdk
import dsh.streams.StreamsParser
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scala.collection.JavaConverters._

/** */
object ConfigMgr {

  @transient private lazy val sdk: Sdk = new Sdk.Builder().autoDetect().build()
  @transient private lazy val kafkaParser = KafkaParser.of(sdk)
  @transient private lazy val streamParser = StreamsParser.of(sdk)

  def producerConfig: Properties = kafkaParser.kafkaProducerProperties(null)

  def consumerConfig: Properties = KafkaParser.addConsumerGroup(
    kafkaParser.suggestedConsumerGroup(ConsumerGroupType.SHARED),
    kafkaParser.kafkaConsumerProperties(null))

  def streams: StreamsParser = streamParser

  def identity: Envelope.Identity = sdk.getApp.identity()

  private lazy val config = ConfigFactory.load()

  lazy val key: String = config.getString("stream.key")
  lazy val input: String = config.getString("stream.in")
  lazy val output: String = config.getString("stream.out")
}
