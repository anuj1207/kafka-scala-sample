package producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}


object MyProducer extends App{

  val key = "myKey"
  val value = "myValue"
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
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  try {
    println("Sending messages to kafka broker")
    for(i <- 1 to 10){
      producer.send(new ProducerRecord[String, String]("myTopic", s"$i",s"hello$i"))
    }
    producer.close()
    println("Data sent")
  }catch {
    case x: Exception => println(s"Yaar ye error hai ::::${x.getMessage}")
  }

}
