/** From a given FlyManager, generate random but coherent data.
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import scala.util.Random

/** Represent a data type
  */
abstract class DataCreator(manager: FlyManager) {
  def create: Unit
}

/** Generate airports inserts from CSV file
  *
  * @param fromFile
  */
case class AirportsGenerator(manager: FlyManager, fromFile: String)
    extends DataCreator(manager) {

  val airports = Utils
    .readLinesFromFile(fromFile)
    .map(l =>
      l.split(",").toList match {
        case i :: n :: _ => Some(Airport(n, i))
        case _           => None
      }
    )
    .flatten

  val iataCodes = airports.map(_.iataCode)

  override def create: Unit = manager.createAirports(airports)
}

/** Generate aircrafts inserts
  *
  * @param number
  * @param minSeats
  * @param maxSeats
  */
case class AircraftsGenerator(
    manager: FlyManager,
    brands: List[String],
    number: Int,
    minSeats: Int,
    maxSeats: Int
) extends DataCreator(manager) {
  val aircrafts = (1 to number).map { id =>
    val brand = brands(Random.nextInt(brands.length))
    Aircraft(id, brand, Utils.randBetween(minSeats, maxSeats))
  }.toList

  def getSeats(aircraftId: Int): Option[Int] =
    aircrafts.find(a => a.id == aircraftId) match {
      case Some(a) => Some(a.seats)
      case None    => None
    }

  override def create: Unit = manager.createAircrafts(aircrafts)
}

/** Generate clients inserts from CSV file
  *
  * @param fromFile
  */
case class ClientsGenerator(manager: FlyManager, fromFile: String)
    extends DataCreator(manager) {

  val clients = Utils
    .readLinesFromFile(fromFile)
    .map(l =>
      l.split(",").toList match {
        case f :: l :: e :: _ => Some((f, l, e))
        case _                => None
      }
    )
    .flatten
    .zipWithIndex
    .map { case ((firstname, lastname, email), id) =>
      Client(id + 1, firstname, lastname, email)
    }

  override def create: Unit = manager.createClients(clients)
}

/** Generate flights inserts from given iataCodes, aircrafts, start and end datetimes
  *
  * @param flightsNumber
  * @param iataCodes
  * @param aircrafts
  * @param startDate
  * @param endDate
  * @param startTime
  * @param endTime
  */
case class FlightsGenerator(
    manager: FlyManager,
    flightsNumber: Int,
    iataCodes: List[String],
    aircrafts: List[Aircraft],
    startDate: LocalDate,
    endDate: LocalDate,
    startTime: LocalTime,
    endTime: LocalTime
) extends DataCreator(manager) {

  import java.time.temporal.ChronoUnit.{DAYS, MINUTES}

  private def getTwoDifferentsIataCodes: (String, String) = {
    val i1 = iataCodes(Random.nextInt(iataCodes.length))
    val i2 = iataCodes(Random.nextInt(iataCodes.length))
    if (i1 == i2) getTwoDifferentsIataCodes
    else (i1, i2)
  }

  private def randomHour(from: LocalTime, to: LocalTime): LocalTime = {
    val diff = MINUTES.between(from, to)
    from.plusMinutes((Random.nextInt(diff.toInt + 1) / 15) * 15)
  }

  private def randomDay(from: LocalDate, to: LocalDate): LocalDate = {
    val diff = DAYS.between(from, to)
    from.plusDays(Random.nextInt(diff.toInt + 1))
  }

  val flights = (1 to flightsNumber).map { id =>
    val date = randomDay(startDate, endDate)
    val time = randomHour(startTime, endTime)
    val (i1, i2) = getTwoDifferentsIataCodes
    val aircraft = aircrafts(Random.nextInt(aircrafts.length))
    Flight(id, LocalDateTime.of(date, time), i1, i2, aircraft.id)
  }.toList

  override def create: Unit = manager.createFlights(flights)
}

/** Generate bookings from aircrafts, flights and clients
  *
  * @param ais
  * @param flights
  * @param clientsNumber
  */
case class Bookings(
    manager: FlyManager,
    ais: AircraftsGenerator,
    flights: List[Flight],
    clientsNumber: Int,
    flightFillRate: Double
) extends DataCreator(manager) {
  require(flightFillRate > 0.0 && flightFillRate <= 1.0)

  import scala.collection.mutable

  private val flightIdClientIds = mutable.Set[(Int, Int)]()
  private val flightIdSeats = mutable.Set[(Int, Int)]()

  private def validBookings(flight: Flight, number: Int): List[Booking] = {
    def loop(ll: LazyList[Booking]): LazyList[Booking] = {
      val seat = Random.nextInt(ais.getSeats(flight.aircraftId).get) + 1
      val clientId = Random.nextInt(clientsNumber) + 1
      if (
        !(flightIdClientIds.contains((flight.id, clientId)) || flightIdSeats
          .contains((flight.id, seat)))
      ) {
        flightIdClientIds.add((flight.id, clientId))
        flightIdSeats.add((flight.id, seat))
        Booking(flight.id, clientId, seat, "pending") #:: loop(ll)
      } else loop(ll)
    }
    loop(LazyList.empty).take(number).toList
  }

  val bookings: List[Booking] = (for {
    flight <- flights
  } yield {
    validBookings(
      flight,
      (flightFillRate * ais.getSeats(flight.aircraftId).get).toInt
    )
  }).flatten

  override def create: Unit = manager.createBookings(bookings)
}

object DataGenerator extends App {

  val clientsNumber = Utils.readLinesFromFile(args(1)).length
  val flightsNumber = clientsNumber / 10
  val brands = Utils.readLinesFromFile(args(2))

  val host = sys.env.get("MYSQL_HOST")
  val port = sys.env.getOrElse("MYSQL_PORT", "3306")
  val user = sys.env.get("MYSQL_USER")
  val password = sys.env.get("MYSQL_PASSWORD")
  val dbName = sys.env.get("MYSQL_DATABASE")
  val kHost = sys.env.get("KAFKA_HOST")
  val kPort = sys.env.get("KAFKA_PORT")

  val manager = (host, port, user, password, dbName, kHost, kPort) match {
    case (Some(h), po, Some(u), Some(pa), Some(d), Some(kh), Some(kp)) =>
      new KafkaFlyManager(kh, kp)
    case _ => sys.exit(42)
  }

  val thread = new Thread(manager)
  thread.start

  val aps = AirportsGenerator(manager, args(0))
  val ais = AircraftsGenerator(manager, brands, 50, 50, 100)
  val cs = ClientsGenerator(manager, args(1))
  val fs = FlightsGenerator(
    manager,
    flightsNumber,
    aps.iataCodes,
    ais.aircrafts,
    LocalDate.of(2021, 1, 1),
    LocalDate.of(2021, 12, 31),
    LocalTime.of(6, 0),
    LocalTime.of(22, 0)
  )
  val bs = Bookings(manager, ais, fs.flights, clientsNumber, 0.9)

  aps.create
  println("Airports done")
  ais.create
  println("Aircrafts done")
  cs.create
  println("Clients done")
  fs.create
  println("Flights done")
  bs.create
  println("Bookings done")

  thread.join
}
