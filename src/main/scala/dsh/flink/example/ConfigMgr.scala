package dsh.flink.example

import java.io.FileInputStream
import java.util.Properties

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/** */
object ConfigMgr {

  lazy val systemProps: Properties = {
    val props = new Properties()
    sys.props.get("platform.properties.file")
      .map(new FileInputStream(_))
      .foreach(props.load)

    props
  }

  private def addCommonKafkaProps(props: Properties): Properties = {
    props.putAll(
      Seq("bootstrap.servers", "security.protocol", "ssl.truststore.location", "ssl.truststore.password", "ssl.keystore.location", "ssl.keystore.password", "ssl.key.password")
        .flatMap(key => Option(systemProps.get(key)).map(key -> _))
        .toMap
        .asJava)
    props
  }

  def onValidConfig[T](props: Properties)(block: => T): Try[T] = {
    Option(props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)).filter(_.nonEmpty).map(_ => Success(block)).getOrElse(Failure(new IllegalArgumentException): Try[T])
  }

  lazy val producerConfig: Properties = {
    addCommonKafkaProps(new Properties)
  }

  lazy val consumerConfig: Properties = {
    val props = addCommonKafkaProps(new Properties)
    Option(systemProps.getProperty("consumerGroups.shared"))
      .map(cgs => cgs.split(',').toSeq.min)
      .foreach(props.put(ConsumerConfig.GROUP_ID_CONFIG, _))

    props
  }
}
