package dsh.flink.example

import java.util.{Optional, Properties}

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import dsh.messages.Envelope.{DataEnvelope, KeyEnvelope, QoS}
import dsh.messages.{DataStream, Envelope}
import dsh.sdk.kafka.partitioners.DshStreamPartitioner
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}

import scala.util.Try

/**
 *
 */
object KafkaUtils extends LazyLogging {
  private def dynamicFlinkPartitioner: FlinkKafkaPartitioner[(String, Clock.Tick)] = new FlinkKafkaPartitioner[(String, Clock.Tick)] {
    private lazy val dyn = new DshStreamPartitioner(ConfigMgr.streams);
    override def partition(record: (String, Clock.Tick), key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int =
      dyn.partition(
        targetTopic,
        record._1,
        partitions.length)
  }

  //TODO: helper functions in SDK to easily build up KeyEnvelopes and DataEnvelopes

  private def produceSerde: KeyedSerializationSchema[(String, Clock.Tick)] = new KeyedSerializationSchema[(String, Clock.Tick)]() {
    override def serializeKey(element: (String, Clock.Tick)): Array[Byte] =
      KeyEnvelope.newBuilder()
        .setKey(element._1)
        .setHeader(Envelope.KeyHeader.newBuilder
          .setIdentifier(ConfigMgr.identity)
          .setQos(QoS.RELIABLE)
          .setRetained(true))
        .build()
        .toByteArray

    override def serializeValue(element: (String, Clock.Tick)): Array[Byte] =
      DataEnvelope.newBuilder()
        .setPayload(ByteString.copyFrom(element._2.ts.toString.getBytes()))
        .build()
        .toByteArray

    override def getTargetTopic(element: (String, Clock.Tick)): String = null
  }

  private def KeyValue(key: Array[Byte], value: Array[Byte]): Try[(KeyEnvelope, DataEnvelope)] = Try(KeyEnvelope.parseFrom(key) -> DataEnvelope.parseFrom(value))

  private def consumerSerde: KeyedDeserializationSchema[Clock.Tick] = new KeyedDeserializationSchema[Clock.Tick] {
    override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): Clock.Tick = {
      logger.info(s"KAFKA: received data - topic:'$topic', partition:$partition, offset:$offset - key-size: ${Option(messageKey).map(_.length).orNull}, value-size: ${Option(message).map(_.length).orNull} ")
      KeyValue(messageKey, message)
        .map { case (keyEnv, dataEnv) if keyEnv.getKey.equals(ConfigMgr.key) => dataEnv.getPayload.toStringUtf8 }
        .flatMap(content => Try(content.toLong))
        .map(Clock.Tick)
        .getOrElse(null)
    }

    override def isEndOfStream(nextElement: Clock.Tick): Boolean = false // it's a nerver ending story
    override def getProducedType: TypeInformation[Clock.Tick] = TypeInformation.of(classOf[Clock.Tick])
  }

  def flinkProducer(props: Properties): FlinkKafkaProducer011[(String, Clock.Tick)] = {
    new FlinkKafkaProducer011[(String, Clock.Tick)](
      ConfigMgr.streams.findStream(DataStream.of(ConfigMgr.output)).get().produceTopic().get(), //FIXME: when not found?
      produceSerde,
      props,
      Optional.of(dynamicFlinkPartitioner))
  }

  def flinkConsumer(props: Properties) = new FlinkKafkaConsumer011[Clock.Tick](
    ConfigMgr.streams.findStream(DataStream.of(ConfigMgr.input)).get().subscribePattern().get(), //FIXME: when not found?
    consumerSerde,
    props)
}
