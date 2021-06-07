package ch.hepia

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.util.Random

import FlyDebeziumDeserializer.eventToFlyModelEvent

object KStreamsTest extends App {

  import Serdes._

  import spray.json._
  import DefaultJsonProtocol._

  implicit val aircraftFormat = jsonFormat3(Aircraft)
  implicit val clientFormat = jsonFormat4(Client)

  val kHost = sys.env.get("KAFKA_HOST").get
  val kPort = sys.env.get("KAFKA_PORT").get

  val props: Properties = {
    val p = new Properties()
    p.put(
      StreamsConfig.APPLICATION_ID_CONFIG,
      LocalDateTime.now().toEpochSecond(ZoneOffset.UTC).toString
    )
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, s"$kHost:$kPort")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder
  val dbzAircrafts: KStream[String, String] =
    builder.stream[String, String]("dbserver.fly.Aircraft")
  val aircraftStream: KStream[Int, String] = dbzAircrafts.map((k, v) => {
    val event = eventToFlyModelEvent(k.parseJson, v.parseJson)
    if (event.isDefined) {
      event.get match {
        case a: AircraftEvent => {
          val aircraft = a.op match {
            case Create | Update => a.after.get
            case Delete          => a.before.get
          }
          (aircraft.id, aircraft.toJson.toString)
        }
        case _ => (-1, "")
      }
    } else (-1, "")
  })
  aircraftStream.to("aircraft-stream")

  val dbzClients: KStream[String, String] =
    builder.stream[String, String]("dbserver.fly.Client")
  val clientStream: KStream[Int, String] = dbzClients.map((k, v) => {
    val event = eventToFlyModelEvent(k.parseJson, v.parseJson)
    if (event.isDefined) {
      event.get match {
        case a: ClientEvent => {
          val client = a.op match {
            case Create | Update => a.after.get
            case Delete          => a.before.get
          }
          (client.id, client.toJson.toString)
        }
        case _ => (-1, "")
      }
    } else (-1, "")
  })
  clientStream.print(Printed.toSysOut)
  clientStream.to("client-stream")

  // clientStream.filter((k, _) => k == 42).print(Printed.toSysOut)

  // val aircraftTable: KTable[Int, String] = builder.table("aircraft-stream")

  // val asdf = aircraftTable.filter((k, v) => k == 50)
  // println(s"filter table : ${asdf}")

  // val plus51 = aircraftTable.filter((k, _) => k > 51)
  // plus51.toStream.print(Printed.toSysOut())
  // plus51.toStream.to("aircraft-table")

  // builder.table[Int, String]("aircraft-table").toStream.print(Printed.toSysOut())

  // dbzAircrafts.foreach((k,v) => println(s"$k, \t $v"))

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close(10, TimeUnit.SECONDS)
  }
}
