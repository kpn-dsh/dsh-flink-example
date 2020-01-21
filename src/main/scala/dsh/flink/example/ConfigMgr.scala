package dsh.flink.example

import java.io.FileInputStream
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

/** */
object ConfigMgr {

  val systemProps = {
    val props = new Properties()
    props.load(new FileInputStream(sys.props.get("platform.properties.file").get))
    props
  }

  lazy val producerConfig: Properties = {
    val props = new Properties()
    Seq("bootstrap.servers", "security.protocol", "ssl.truststore.location", "ssl.truststore.password", "ssl.keystore.location", "ssl.keystore.password", "ssl.key.password").foreach { key => props.put(key, systemProps.get(key)) }
    props
  }

  lazy val consumerConfig: Properties = {
    val props = new Properties()
    Seq("bootstrap.servers", "security.protocol", "ssl.truststore.location", "ssl.truststore.password", "ssl.keystore.location", "ssl.keystore.password", "ssl.key.password").foreach { key => props.put(key, systemProps.get(key)) }
    props.put(ConsumerConfig.GROUP_ID_CONFIG, systemProps.getProperty("consumerGroups.shared").split(',').toSeq.min)
    props
  }
}
