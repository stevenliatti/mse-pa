/** Kafka only implementation
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.time.LocalDateTime
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class KafkaFlyManager(kafkaHost: String, kafkaPort: String)
    extends FlyManager
    with Runnable {

  private val props = {
    val serializer = "org.apache.kafka.common.serialization.StringSerializer"
    val p = new Properties()
    p.put("bootstrap.servers", s"$kafkaHost:$kafkaPort")
    p.put("acks", "all")
    p.put("retries", 0)
    p.put("linger.ms", 1)
    p.put("key.serializer", serializer)
    p.put("value.serializer", serializer)
    p
  }

  private val producer = new KafkaProducer[String, String](props)

  private val aircraftsMap = mutable.Map[Int, Aircraft]()
  private val airportsMap = mutable.Map[String, Airport]()
  private val bookingsMap =
    mutable.Map[(Int, Int), (Array[BookingHistory], Status)]()
  private val clientsMap = mutable.Map[Int, (Array[ClientHistory], Status)]()
  private val flightsMap = mutable.Map[Int, (Array[FlightHistory], Status)]()
  private val flightsSeatsMap =
    mutable.Map[(Int, Int), (Array[BookingHistory], Status)]()

  private val aircraftTopic = "Aircraft"
  private val airportTopic = "Airport"
  private val bookingTopic = "Booking"
  private val clientTopic = "Client"
  private val flightTopic = "Flight"
  private val topics =
    List(aircraftTopic, airportTopic, bookingTopic, clientTopic, flightTopic)
  private val separator = "#"

  private def serialize(
      op: Operation,
      data: Fly
  ): (String, String) = {
    val opStr = op match {
      case Create => "c"
      case Update => "u"
      case Delete => "d"
    }
    data match {
      case Aircraft(id, brand, seats) =>
        (s"$id", s"$opStr$separator$id$separator$brand$separator$seats")
      case Airport(iataCode, name) =>
        (s"$iataCode", s"$opStr$separator$iataCode$separator$name")
      case Booking(fId, cId, seat, state) =>
        (
          s"$fId$separator$cId",
          s"$opStr$separator$fId$separator$cId$separator$seat$separator$state"
        )
      case Client(id, f, l, e) =>
        (s"$id", s"$opStr$separator$id$separator$f$separator$l$separator$e")
      case Flight(id, dt, dIata, aIata, aId) =>
        (
          s"$id",
          s"$opStr$separator$id$separator$dt$separator$dIata$separator$aIata$separator$aId"
        )
    }
  }

  private def writeTo(topic: String, op: Operation, data: Fly) = {
    val (key, value) = serialize(op, data)
    producer.send(
      new ProducerRecord[String, String](topic, key, value)
    )
  }

  // TODO
  private def deserializeAircraft(
      key: String,
      value: String
  ): Try[(Operation, Aircraft)] = ???
  private def deserializeAirport(
      key: String,
      value: String
  ): Try[(Operation, Airport)] = ???
  private def deserializeBooking(
      key: String,
      value: String
  ): Try[(Operation, Booking)] = ???
  private def deserializeClient(
      key: String,
      value: String
  ): Try[(Operation, Client)] = ???
  private def deserializeFlight(
      key: String,
      value: String
  ): Try[(Operation, Flight)] = ???

  private def readFrom(
      record: ConsumerRecord[String, String]
  ): Option[(Operation, Fly)] = {
    record.topic match {
      case `aircraftTopic` => {
        deserializeAircraft(record.key, record.value) match {
          case Success(value) => Some(value)
          case Failure(_)     => None
        }
      }
      case `airportTopic` => {
        deserializeAirport(record.key, record.value) match {
          case Success(value) => Some(value)
          case Failure(_)     => None
        }
      }
      case `bookingTopic` => {
        deserializeBooking(record.key, record.value) match {
          case Success(value) => Some(value)
          case Failure(_)     => None
        }
      }
      case `clientTopic` => {
        deserializeClient(record.key, record.value) match {
          case Success(value) => Some(value)
          case Failure(_)     => None
        }
      }
      case `flightTopic` => {
        deserializeFlight(record.key, record.value) match {
          case Success(value) => Some(value)
          case Failure(_)     => None
        }
      }
      case _ => None
    }
  }

  override def run(): Unit = {
    val properties = new Properties()
    val deserializer =
      "org.apache.kafka.common.serialization.StringDeserializer"
    properties.put("bootstrap.servers", s"$kafkaHost:$kafkaPort")
    properties.put("key.deserializer", deserializer)
    properties.put("value.deserializer", deserializer)
    properties.put("group.id", "kafka")

    val consumer = new KafkaConsumer[String, String](properties)

    consumer.subscribe(topics.asJava)

    consumer.poll(0) // dummy poll() to join consumer group
    consumer.seekToBeginning(consumer.assignment)

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        val maybeFlyEvent = readFrom(record)
        if (maybeFlyEvent.isDefined) {
          val (op, flyEvent) = maybeFlyEvent.get
          flyEvent match {
            case Aircraft(id, brand, seats) =>
              op match {
                case Create => aircraftsMap.put(id, Aircraft(id, brand, seats))
                case _      => ()
              }
            case Airport(iataCode, name) =>
              op match {
                case Create =>
                  airportsMap.put(iataCode, Airport(iataCode, name))
                case _ => ()
              }
            // TODO update two maps bookingsMap and flightsSeatsMap
            case Booking(fId, cId, seat, state) =>
            case Client(id, f, l, e) =>
              op match {
                case Create => {
                  val now = LocalDateTime.now
                  val ch = ClientHistory(id, f, l, e, now, now, now)
                  clientsMap.put(id, (Array(ch), PRESENT))
                }
                case Update => {
                  val now = LocalDateTime.now
                  val oldCh = clientsMap(id)._1.last
                  val tsFirstname =
                    if (f != oldCh.firstname) now else oldCh.tsFirstname
                  val tsLastname =
                    if (l != oldCh.lastname) now else oldCh.tsFirstname
                  val tsEmail = if (e != oldCh.email) now else oldCh.tsFirstname
                  val newCh =
                    ClientHistory(id, f, l, e, tsFirstname, tsLastname, tsEmail)
                  clientsMap.put(id, (clientsMap(id)._1 :+ newCh, PRESENT))
                }
                case Delete => clientsMap.put(id, (clientsMap(id)._1, DELETED))
              }
            case Flight(id, dt, dIata, aIata, aId) => // TODO
          }
        }
      }
    }
    Try(consumer.close()).recover { case error =>
      println("Failed to close the kafka consumer", error)
    }
  }

  private def create(fly: Fly, topic: String, containInMap: Boolean): Unit =
    if (containInMap) writeTo(topic, Create, fly)

  override def createAirport(a: Airport): Unit =
    create(a, airportTopic, !airportsMap.contains(a.iataCode))

  override def createAircraft(a: Aircraft): Unit =
    create(a, aircraftTopic, !aircraftsMap.contains(a.id))

  override def createClient(c: Client): Unit =
    create(c, clientTopic, !clientsMap.contains(c.id))

  override def createFlight(f: Flight): Unit =
    create(f, flightTopic, !flightsMap.contains(f.id))

  override def createBooking(b: Booking): Unit =
    create(b, bookingTopic, !bookingsMap.contains((b.flightId, b.clientId)))

  override def createAirports(as: List[Airport]): Unit =
    as.foreach(a => createAirport(a))

  override def createAircrafts(as: List[Aircraft]): Unit =
    as.foreach(a => createAircraft(a))

  override def createClients(cs: List[Client]): Unit =
    cs.foreach(c => createClient(c))

  override def createFlights(fs: List[Flight]): Unit =
    fs.foreach(f => createFlight(f))

  override def createBookings(bs: List[Booking]): Unit =
    bs.foreach(b => createBooking(b))

  private def update(newFly: Fly, topic: String, containInMap: Boolean): Unit =
    if (containInMap) writeTo(topic, Update, newFly)

  override def updateBookingSeat(b: Booking, newSeatNumber: Int): Unit =
    update(
      Booking(b.flightId, b.clientId, newSeatNumber, b.state),
      bookingTopic,
      !flightsSeatsMap.contains((b.flightId, newSeatNumber))
    )

  override def updateBookingState(b: Booking, newState: String): Unit = {
    update(
      Booking(b.flightId, b.clientId, b.seat, newState),
      bookingTopic,
      bookingsMap.contains((b.flightId, b.clientId))
    )
  }

  override def updateClientFirstname(c: Client, newFirstname: String): Unit = {
    update(
      Client(c.id, newFirstname, c.lastname, c.email),
      clientTopic,
      clientsMap.contains(c.id)
    )
  }

  override def updateClientLastname(c: Client, newLastname: String): Unit = {
    update(
      Client(c.id, c.firstname, newLastname, c.email),
      clientTopic,
      clientsMap.contains(c.id)
    )
  }

  override def updateClientEmail(c: Client, newEmail: String): Unit = {
    update(
      Client(c.id, c.firstname, c.lastname, newEmail),
      clientTopic,
      clientsMap.contains(c.id)
    )
  }

  override def updateFlightDatetime(
      f: Flight,
      newDatetime: LocalDateTime
  ): Unit = {
    update(
      Flight(
        f.id,
        newDatetime,
        f.departureIataCode,
        f.arrivalIataCode,
        f.aircraftId
      ),
      flightTopic,
      flightsMap.contains(f.id)
    )
  }

  override def deleteBooking(b: Booking): Unit =
    writeTo(bookingTopic, Delete, b)

  // TODO problem -> transactions
  override def deleteFlight(f: Flight): Unit = ???

  override def client(id: Int): Option[(Client, Status)] =
    clientsMap.get(id) match {
      case Some((clients, status)) => Some((clients.last.toClient, status))
      case None                    => None
    }

  override def aircraft(id: Int): Option[Aircraft] = aircraftsMap.get(id)

  override def flight(id: Int): Option[(Flight, Status)] =
    flightsMap.get(id) match {
      case Some((flights, status)) => Some((flights.last.toFlight, status))
      case None                    => None
    }

  override def client(
      flightId: Int,
      seatNumber: Int
  ): Option[(Client, Status)] =
    flightsSeatsMap.get((flightId, seatNumber)) match {
      case Some((bookings, status)) =>
        Some((client(bookings.last.clientId).get._1, status))
      case None => None
    }

  override def seatNumber(flightId: Int, clientId: Int): Option[(Int, Status)] =
    bookingsMap.get((flightId, clientId)) match {
      case Some((bookings, status)) => Some((bookings.last.seat, status))
      case None                     => None
    }

  override def clients: List[Client] = clientsMap.values
    .filter { case (_, status) => status == PRESENT }
    .map { case (chs, _) => chs.last.toClient }
    .toList

  override def flights: List[Flight] = flightsMap.values
    .filter { case (_, status) => status == PRESENT }
    .map { case (fhs, _) => fhs.last.toFlight }
    .toList

  override def airports: List[Airport] = airportsMap.values.toList

  override def aircrafts: List[Aircraft] = aircraftsMap.values.toList

  override def aircraftsBrands: List[String] =
    aircraftsMap.values.map(_.brand).toSet.toList

  override def mostFlyableAircraftsBrands: Map[String, Int] =
    flightsMap
      .filter { case (_, v) => v._2 == PRESENT }
      .map { case (k, v) =>
        (k, aircraftsMap(v._1.last.aircraftId).brand)
      }
      .values
      .groupBy(b => b)
      .map { case (brand, v) => (brand, v.toList.length) }

  override def avgAircraftSeats: Double = aircraftsMap.map { case (k, v) =>
    v.seats
  }.sum / aircraftsMap.size

  override def clientsEmail: List[String] = clientsMap.values
    .filter { case (_, status) => status == PRESENT }
    .map { case (chs, _) => chs.last.email }
    .toList

  override def totalSeatsNumber(f: Flight): Option[Int] =
    flightsMap.get(f.id).filter { case (_, status) => status == PRESENT }.map {
      case (fhs, _) => aircraftsMap(fhs.last.aircraftId).seats
    }

  override def availableSeats(f: Flight): List[Int] =
    totalSeatsNumber(f) match {
      case Some(totalSeats) => {
        val takenSeats = bookingsMap
          .filter { case ((fId, _), (_, status)) =>
            fId == f.id && status == PRESENT
          }
          .values
          .map { case (bhs, _) => bhs.last.seat }
          .toSet
        val allSeats = (1 to totalSeats).toSet
        (allSeats diff takenSeats).toList.sorted
      }
      case None => Nil
    }

  override def avgFlightOccupancy(f: Flight): Option[Double] =
    totalSeatsNumber(f).map(t => availableSeats(f).length / t)

  override def avgFlightsOccupancies: Map[Int, Double] = flightsMap.map {
    case (k, v) => (k, avgFlightOccupancy(v._1.last.toFlight).get)
  }.toMap

  override def clientFlights(c: Client): List[Flight] = bookingsMap
    .filter { case ((_, cId), (_, status)) =>
      cId == c.id && status == PRESENT
    }
    .values
    .map { case (bhs, _) => bhs.last.flightId }
    .map(fId => flight(fId).get._1)
    .toList

  override def airportsWithMostDepartures: Map[Airport, Int] = flightsMap.values
    .filter { case (_, status) => status == PRESENT }
    .map { case (fhs, _) => fhs.last.departureIataCode }
    .groupBy(d => d)
    .map { case (dIata, v) => (airportsMap(dIata), v.toList.length) }

  override def airportsWithMostArrivals: Map[Airport, Int] = flightsMap.values
    .filter { case (_, status) => status == PRESENT }
    .map { case (fhs, _) => fhs.last.arrivalIataCode }
    .groupBy(d => d)
    .map { case (dIata, v) => (airportsMap(dIata), v.toList.length) }

  override def clientHistory(clientId: Int): (List[ClientHistory], Status) =
    clientsMap.get(clientId) match {
      case Some((chs, status)) => (chs.toList, status)
      case None                => (Nil, NEVER_PRESENT)
    }

  override def clientsHistory: Map[Int, (List[ClientHistory], Status)] =
    clientsMap.map { case (k, _) => (k, clientHistory(k)) }.toMap

  override def bookingHistory(
      flightId: Int,
      clientId: Int
  ): (List[BookingHistory], Status) =
    bookingsMap.get((flightId, clientId)) match {
      case Some((bhs, status)) => (bhs.toList, status)
      case None                => (Nil, NEVER_PRESENT)
    }

  override def flightHistory(flightId: Int): (List[FlightHistory], Status) =
    flightsMap.get(flightId) match {
      case Some((fhs, status)) => (fhs.toList, status)
      case None                => (Nil, NEVER_PRESENT)
    }

}
