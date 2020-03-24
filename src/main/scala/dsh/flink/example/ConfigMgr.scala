package dsh.flink.example

import java.util.Properties

import com.typesafe.config.ConfigFactory
import dsh.messages.Envelope
import dsh.sdk.Sdk
import dsh.sdk.kafka.KafkaConfigParser
import dsh.sdk.kafka.KafkaConfigParser.ConsumerGroupType
import dsh.sdk.streams.StreamsConfigParser

/** */
object ConfigMgr {

  @transient private lazy val sdk: Sdk = new Sdk.Builder().autoDetect().build()
  @transient private lazy val kafkaParser = KafkaConfigParser.of(sdk)
  @transient private lazy val streamParser = StreamsConfigParser.of(sdk)

  def producerConfig: Properties = kafkaParser.kafkaProducerProperties(null)

  def consumerConfig: Properties = KafkaConfigParser.addConsumerGroup(
    kafkaParser.suggestedConsumerGroup(ConsumerGroupType.SHARED),
    kafkaParser.kafkaConsumerProperties(null))

  def streams: StreamsConfigParser = streamParser

  def identity: Envelope.Identity = sdk.getApp.identity()

  private lazy val config = ConfigFactory.load()

  lazy val key: String = config.getString("stream.key")
  lazy val input: String = config.getString("stream.in")
  lazy val output: String = config.getString("stream.out")
}
