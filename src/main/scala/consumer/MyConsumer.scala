package consumer

import java.io.StreamCorruptedException
import java.util
import java.util.Properties

import models.User
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._

object MyConsumer extends App{

  val topic = "sampleTopic"
  val props = new Properties()
  //Kafka consumer configuration settings
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "serialization.UserDeserializer")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[String, User](props)

  consumer.subscribe(util.Collections.singletonList(topic))
  while(true){
      val records = consumer.poll(100)
      for(record<-records.asScala)
        println(s"offset: ${record.offset()}::::key:${record.key()}:::value:${record.value()}git ")
  }

}
