package ch.hepia

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

object ConsumerExample extends App {

  val topic = "dbserver1.fly.Aircraft"

  val properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put(
    "key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  properties.put(
    "value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  properties.put("group.id", "something")
  // props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer[String, String](properties)

  consumer.subscribe(Collections.singletonList(topic))

  consumer.poll(0) // dummy poll() to join consumer group
  consumer.seekToBeginning(consumer.assignment)

  // while (true) {
    val records = consumer.poll(100)
    for (record <- records.asScala) {
      println(record)
    }
  // }
}
