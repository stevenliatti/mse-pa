/** MySQL + Debezium + KStreams implementation.
  *
  * Not functional
  *
  * @author Steven Liatti
  * @version 0.1
  */

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

class MySQLDebeziumKStreamsFlyManager(
    mySqlHost: String,
    mySqlPort: String,
    mySqlUser: String,
    mySqlPassword: String,
    dbName: String,
    kafkaHost: String,
    kafkaPort: String,
    dbServerName: String
) extends MySQLDebeziumFlyManager(
      mySqlHost,
      mySqlPort,
      mySqlUser,
      mySqlPassword,
      dbName,
      kafkaHost,
      kafkaPort,
      dbServerName
    )
    with Runnable {

  import Serdes._
  import spray.json._
  import DefaultJsonProtocol._

  implicit val aircraftFormat = jsonFormat3(Aircraft)
  implicit val clientFormat = jsonFormat4(Client)

  val props: Properties = {
    val p = new Properties()
    p.put(
      StreamsConfig.APPLICATION_ID_CONFIG,
      LocalDateTime.now().toEpochSecond(ZoneOffset.UTC).toString
    )
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, s"$kafkaHost:$kafkaPort")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

  override def run: Unit = {

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
    clientStream.to("client-stream")

    val clientTable: KTable[Int, String] = builder.table("client-stream")
    clientTable.toStream.to("client-table")

    clientStream.print(Printed.toSysOut)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.cleanUp()
    streams.start()

    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
    }

    println("streams started")
  }

  override def client(id: Int): Option[(Client, Status)] = {
    val builder = new StreamsBuilder
    var clientStates = Array[Client]()
    builder
      .stream[Int, String]("client-stream")
      .filter((k, _) => k == id)
      .foreach((k, v) => {
        val c = v.parseJson.convertTo[Client]
        clientStates = clientStates :+ c
      })
    var present = false
    builder
      .table[Int, String]("client-table")
      .filter((k, _) => k == id)
      .toStream
      .foreach((k, v) => present = true)

    (clientStates.nonEmpty, present) match {
      case (true, true)   => Some((clientStates.last, PRESENT))
      case (true, false)  => Some((clientStates.last, DELETED))
      case (false, false) => Some((clientStates.last, NEVER_PRESENT))
      case _              => None
    }
  }
}
