import java.util
import java.util.{Collections, Properties}
import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.KafkaConsumer

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    val topic = "test"
    props.put("bootstrap.servers", "127.0.0.1:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val kafkaConsumer = new KafkaConsumer[String, String](props)
    kafkaConsumer.subscribe(Collections.singletonList(topic))
    while (true) {
      val records = kafkaConsumer.poll(1000)
      for (record <- records.iterator()){
        println(record)
      }
    }

  }
}
