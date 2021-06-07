/** MySQL + Debezium implementation
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import org.apache.kafka.clients.consumer.KafkaConsumer
import spray.json.JsNumber
import spray.json.JsObject
import spray.json.JsString
import spray.json.JsValue

import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.Properties
import java.util.TimeZone
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

class MySQLDebeziumFlyManager(
    mySqlHost: String,
    mySqlPort: String,
    mySqlUser: String,
    mySqlPassword: String,
    dbName: String,
    kafkaHost: String,
    kafkaPort: String,
    dbServerName: String
) extends MySQLFlyManager(
      mySqlHost,
      mySqlPort,
      mySqlUser,
      mySqlPassword,
      dbName
    )
    with Runnable {

  import spray.json._
  import DefaultJsonProtocol._ // if you don't supply your own Protocol (see below)

  private val bookingsMap = mutable.Map[(Int, Int), Array[BookingHistory]]()
  private val clientsMap = mutable.Map[Int, Array[ClientHistory]]()
  private val flightsMap = mutable.Map[Int, Array[FlightHistory]]()
  private val flightsSeatsMap = mutable.Map[(Int, Int), Array[BookingHistory]]()

  override def run = {
    val topics =
      List("Booking", "Client", "Flight").map(t => s"$dbServerName.$dbName.$t")

    val properties = new Properties()
    val serializer =
      "org.apache.kafka.common.serialization.StringDeserializer"
    properties.put("bootstrap.servers", s"$kafkaHost:$kafkaPort")
    properties.put("key.deserializer", serializer)
    properties.put("value.deserializer", serializer)
    properties.put("group.id", "debezium")

    val consumer = new KafkaConsumer[String, String](properties)

    consumer.subscribe(topics.asJava)

    consumer.poll(0) // dummy poll() to join consumer group
    consumer.seekToBeginning(consumer.assignment)

    def eventByOp[T](op: Operation, before: Option[T], after: Option[T]) = {
      op match {
        case Create | Update => after.get
        case Delete          => before.get
      }
    }

    def addOrUpdate[K, V: ClassTag](
        map: mutable.Map[K, Array[V]],
        key: K,
        value: V
    ) = {
      if (map.contains(key)) {
        val newArray = map(key) :+ value
        map.put(key, newArray)
      } else {
        map.put(key, Array(value))
      }
    }

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        val key = record.key.parseJson
        val value = Try(record.value.parseJson)
        if (value.isSuccess) {
          val event =
            FlyDebeziumDeserializer.eventToFlyModelEvent(key, value.get)
          if (event.isDefined) {
            event.get match {
              case BookingEvent(before, after, op, timestamp) => {
                val b = eventByOp(op, before, after)
                val bh = BookingHistory(
                  b.flightId,
                  b.clientId,
                  b.seat,
                  b.state,
                  timestamp,
                  timestamp
                )
                addOrUpdate(bookingsMap, (b.flightId, b.clientId), bh)
                addOrUpdate(flightsSeatsMap, (b.flightId, b.seat), bh)
              }
              case ClientEvent(before, after, op, timestamp) => {
                val c = eventByOp(op, before, after)
                val ch = ClientHistory(
                  c.id,
                  c.firstname,
                  c.lastname,
                  c.email,
                  timestamp,
                  timestamp,
                  timestamp
                )
                addOrUpdate(clientsMap, c.id, ch)
              }
              case FlightEvent(before, after, op, timestamp) => {
                val f = eventByOp(op, before, after)
                val fh = FlightHistory(
                  f.id,
                  f.datetime,
                  f.departureIataCode,
                  f.arrivalIataCode,
                  f.aircraftId,
                  timestamp
                )
                addOrUpdate(flightsMap, f.id, fh)
              }
              case _ => ()
            }
          }
        }
      }
    }
    Try(consumer.close()).recover { case error =>
      println("Failed to close the kafka consumer", error)
    }
  }

  // Commands
  def createClient(c: Client): Unit = {
    command(s"""INSERT INTO Client VALUES
      (${c.id}, "${c.firstname}", "${c.lastname}", "${c.email}")""")
  }
  def createFlight(f: Flight): Unit = {
    val vs = s"""(
        ${f.id}, "${f.datetime}", "${f.departureIataCode}",
        "${f.arrivalIataCode}", ${f.aircraftId}
      )"""
    command(s"INSERT INTO Flight VALUES $vs")
  }
  def createBooking(b: Booking): Unit = {
    command(
      s"""INSERT INTO Booking VALUES
        (${b.flightId}, ${b.clientId}, ${b.seat}, "${b.state}")"""
    )
  }

  def createClients(cs: List[Client]): Unit = {
    val vs = cs.map(c =>
      s"""(${c.id}, "${c.firstname}", "${c.lastname}", "${c.email}")"""
    )
    command(
      s"INSERT INTO Client VALUES ${vs.mkString(",")}"
    )
  }
  def createFlights(fs: List[Flight]): Unit = {
    val vs = fs
      .map(f => s"""(
        ${f.id}, "${f.datetime}", "${f.departureIataCode}",
        "${f.arrivalIataCode}", ${f.aircraftId}
      )""")
    command(
      s"INSERT INTO Flight VALUES ${vs.mkString(",")}"
    )
  }
  def createBookings(bs: List[Booking]): Unit = {
    val vs = bs
      .map(b => s"""(${b.flightId}, ${b.clientId}, ${b.seat}, "${b.state}")""")
    command(
      s"INSERT INTO Booking VALUES ${vs.mkString(",")}"
    )
  }

  def updateBookingSeat(b: Booking, newSeatNumber: Int): Unit = {
    command(
      s"""UPDATE Booking SET seat_number = $newSeatNumber
          WHERE flight_id = ${b.flightId} AND client_id = ${b.clientId}"""
    )
  }

  def updateBookingState(b: Booking, newState: String): Unit = {
    command(
      s"""UPDATE Booking SET state = "$newState"
          WHERE flight_id = ${b.flightId} AND client_id = ${b.clientId}"""
    )
  }
  def updateClientFirstname(c: Client, newFirstname: String): Unit = {
    command(
      s"""UPDATE Client SET firstname = "$newFirstname" WHERE id = ${c.id}"""
    )
  }
  def updateClientLastname(c: Client, newLastname: String): Unit = {
    command(
      s"""UPDATE Client SET lastname = "$newLastname" WHERE id = ${c.id}"""
    )
  }
  def updateClientEmail(c: Client, newEmail: String): Unit = {
    command(
      s"""UPDATE Client SET email = "$newEmail" WHERE id = ${c.id}"""
    )
  }
  def updateFlightDatetime(
      f: Flight,
      newDatetime: LocalDateTime
  ): Unit = {
    command(
      s"""UPDATE Flight SET datetime = "$newDatetime" WHERE id = ${f.id}"""
    )
  }

  def deleteBooking(b: Booking): Unit = {
    command(
      s"""DELETE FROM Booking
        WHERE flight_id = ${b.flightId}
        AND client_id = ${b.clientId}"""
    )
  }
  def deleteFlight(f: Flight): Unit =
    try {
      connection.setAutoCommit(false)
      connection.createStatement.executeUpdate(
        s"DELETE FROM Booking WHERE flight_id = ${f.id}"
      )
      connection.createStatement.executeUpdate(
        s"DELETE FROM Flight WHERE id = ${f.id}"
      )
      connection.commit
    } catch {
      case e: Throwable => {
        println(e)
        connection.rollback
      }
    } finally {
      connection.setAutoCommit(true)
    }

  // Queries
  private def query[T, U](
      s: String,
      f: ResultSet => T,
      lookInTopic: U => Option[T],
      arg: U
  ): Option[(T, Status)] = {
    super.query(s, f) match {
      case Some(v) => Some(v, PRESENT)
      case None => {
        val inTopic = lookInTopic(arg)
        if (inTopic.isDefined) Some(inTopic.get, DELETED)
        else None
      }
    }
  }

  private def lookInClientTopic(id: Int): Option[Client] = clientsMap
    .get(id)
    .flatMap(cs => if (cs.nonEmpty) Some(cs.last.toClient) else None)
  private def lookInFlightTopic(id: Int): Option[Flight] = flightsMap
    .get(id)
    .flatMap(fs => if (fs.nonEmpty) Some(fs.last.toFlight) else None)
  private def lookInBookingTopic(flightIdclientId: (Int, Int)): Option[Int] =
    bookingsMap
      .get(flightIdclientId)
      .flatMap(bs => if (bs.nonEmpty) Some(bs.last.seat) else None)
  private def lookInBookingTopic2(
      flightIdSeatNumber: (Int, Int)
  ): Option[Client] = flightsSeatsMap
    .get(flightIdSeatNumber)
    .flatMap(bs =>
      if (bs.nonEmpty) lookInClientTopic(bs.last.clientId) else None
    )

  def client(id: Int): Option[(Client, Status)] =
    query(
      s"SELECT * FROM Client WHERE id = $id",
      Utils.rsToClient,
      lookInClientTopic,
      id
    )

  def flight(id: Int): Option[(Flight, Status)] =
    query(
      s"SELECT * FROM Flight WHERE id = $id",
      Utils.rsToFlight,
      lookInFlightTopic,
      id
    )

  def client(flightId: Int, seatNumber: Int): Option[(Client, Status)] =
    query(
      s"""
        SELECT id, firstname, lastname, email FROM Client
        JOIN Booking ON Booking.client_id = Client.id
        WHERE flight_id = $flightId AND seat_number = $seatNumber
      """,
      Utils.rsToClient,
      lookInBookingTopic2,
      (flightId, seatNumber)
    )
  def seatNumber(flightId: Int, clientId: Int): Option[(Int, Status)] =
    query(
      s"""
        SELECT seat_number FROM Booking
        WHERE flight_id = $flightId AND client_id = $clientId
      """,
      Utils.rsToInt,
      lookInBookingTopic,
      (flightId, clientId)
    )

  def clients: List[Client] =
    queryToList("SELECT * FROM Client", Utils.rsToClient)

  def flights: List[Flight] =
    queryToList("SELECT * FROM Flight", Utils.rsToFlight)

  def clientsEmail: List[String] =
    queryToList("SELECT email FROM Client", Utils.rsToString)

  def clientFlights(c: Client): List[Flight] =
    queryToList(
      s"""
        SELECT * FROM Flight
        JOIN Booking ON id = flight_id
        WHERE client_id = ${c.id}
      """,
      Utils.rsToFlight
    )

  /* Historiques
  Regarder dans le topic Kafka correspondant à la table et aux éventuels critères
  Ensuite trois cas :
    1) Pas de données -> (Nil, NEVER_PRESENT)
    2) Des données et requête à MySQL non nulle -> (theList, PRESENT)
    3) Des données et requête à MySQL nulle -> (theList, DELETED)
   */
  def clientHistory(clientId: Int): (List[ClientHistory], Status) =
    clientsMap.get(clientId) match {
      case Some(cs) =>
        if (cs.nonEmpty) {
          val actualClient = query(
            s"SELECT * FROM Client WHERE id = $clientId",
            Utils.rsToClient
          )
          actualClient match {
            case Some(_) => (cs.toList, PRESENT)
            case None    => (cs.toList, DELETED)
          }
        } else (Nil, NEVER_PRESENT)
      case None => (Nil, NEVER_PRESENT)
    }
  def clientsHistory: Map[Int, (List[ClientHistory], Status)] = {
    query(
      "SELECT id FROM Client ORDER BY id DESC LIMIT 1",
      Utils.rsToInt
    ) match {
      case Some(lastId) =>
        (1 to lastId).map(id => (id, clientHistory(id))).toMap
      case None => Map.empty
    }
  }
  def bookingHistory(
      flightId: Int,
      clientId: Int
  ): (List[BookingHistory], Status) =
    bookingsMap.get((flightId, clientId)) match {
      case Some(bs) =>
        if (bs.nonEmpty) {
          val actual = query(
            s"""
              SELECT * FROM Booking
              WHERE flight_id = $flightId AND client_id = $clientId
            """,
            Utils.rsToBooking
          )
          actual match {
            case Some(_) => (bs.toList, PRESENT)
            case None    => (bs.toList, DELETED)
          }
        } else (Nil, NEVER_PRESENT)
      case None => (Nil, NEVER_PRESENT)
    }
  def flightHistory(flightId: Int): (List[FlightHistory], Status) =
    flightsMap.get(flightId) match {
      case Some(fs) =>
        if (fs.nonEmpty) {
          val actual = query(
            s"SELECT * FROM Flight WHERE id = $flightId",
            Utils.rsToFlight
          )
          actual match {
            case Some(_) => (fs.toList, PRESENT)
            case None    => (fs.toList, DELETED)
          }
        } else (Nil, NEVER_PRESENT)
      case None => (Nil, NEVER_PRESENT)
    }
}
