package consumer

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

object MyConsumer extends App{

  val props = new Properties()
  //Kafka consumer configuration settings
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Arrays.asList("myTopic"))
  while(true){
    val records = consumer.poll(100)
    val itr = records.iterator()
    while(itr.hasNext){
      val record = itr.next()
      println(s"offset: ${record.offset()}::::key:${record.key()}:::value:${record.value()}:::")
    }
  }

}
