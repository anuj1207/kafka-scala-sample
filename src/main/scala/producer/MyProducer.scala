package producer

import java.util.Properties

import models.User
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object MyProducer extends App{

  /*val key = "myKey"
  val value = "myValue"*/
  val topic = "sampleTopic"
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  //Assign localhost id
  props.put("acks", "all")
  //Set acknowledgements for producer requests.
  props.put("retries", "0")
  //If the request fails, the producer can automatically retry,
  props.put("batch.size", "16384")
  //Specify buffer size in config
  props.put("linger.ms", "1")
  //Reduce the no of requests less than 0
  props.put("buffer.memory", "33554432")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "serialization.UserSerializer")

  val producer = new KafkaProducer[String, User](props)

  try {
    println("Sending messages to kafka broker")
    for(i <- 1 to 10){
      val key = i.toString
      val value = User(s"sampleName $i")
      val record = new ProducerRecord[String, User](topic, key, value)
      producer.send(record)
      println(s"Data sent::${value.name}")
    }
    producer.close()
    println("Data sending finished")
  }catch {
    case x: Exception => println(s"Error ::::${x.getMessage}")
  }

}
