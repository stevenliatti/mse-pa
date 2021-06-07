/** MySQL abstract base implementation.
  *
  * Offer the base for other MySQL*FlyManager implementations.
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import java.sql.DriverManager
import java.sql.ResultSet

abstract class MySQLFlyManager(
    host: String,
    port: String,
    user: String,
    password: String,
    dbName: String
) extends FlyManager {

  protected val connection = DriverManager.getConnection(
    s"jdbc:mysql://$host:$port/$dbName",
    user,
    password
  )

  protected def command(s: String): Unit = try {
    connection.createStatement.executeUpdate(s)
  } catch {
    case _: Throwable => ()
  }
  protected def queryToList[T](s: String, f: ResultSet => T): List[T] = {
    def next(l: List[T], rs: ResultSet): List[T] = rs.next match {
      case true  => f(rs) :: next(l, rs)
      case false => l
    }
    try {
      val rs = connection.createStatement.executeQuery(s)
      next(Nil, rs)
    } catch {
      case _: Throwable => Nil
    }
  }
  protected def query[T](s: String, f: ResultSet => T): Option[T] =
    queryToList(s, f) match {
      case h :: _ => Some(h)
      case Nil    => None
    }

  def createAirport(a: Airport): Unit = {
    command(
      s"""INSERT INTO Airport VALUES ("${a.iataCode}", "${a.name}")"""
    )
  }
  def createAircraft(a: Aircraft): Unit = {
    command(
      s"""INSERT INTO Aircraft VALUES (${a.id}, "${a.brand}", ${a.seats})"""
    )
  }

  def createAirports(as: List[Airport]): Unit = {
    val vs =
      as.map(a => s"""("${a.iataCode}", "${a.name}")""").mkString(",")
    command(s"INSERT INTO Airport VALUES $vs")
  }
  def createAircrafts(as: List[Aircraft]): Unit = {
    val vs =
      as.map(a => s"""(${a.id}, "${a.brand}", ${a.seats})""").mkString(",")
    command(s"INSERT INTO Aircraft VALUES $vs")
  }

  def aircraft(id: Int): Option[Aircraft] = {
    query(
      s"SELECT * FROM Aircraft WHERE id = $id",
      Utils.rsToAircraft
    )
  }
  def airports: List[Airport] = {
    queryToList(
      "SELECT * FROM Airport",
      Utils.rsToAirport
    )
  }
  def aircrafts: List[Aircraft] = {
    queryToList(
      "SELECT * FROM Aircraft",
      Utils.rsToAircraft
    )
  }

  def aircraftsBrands: List[String] = {
    queryToList(
      "SELECT DISTINCT brand FROM Aircraft",
      Utils.rsToString
    )
  }

  /** Returns the brands of aircraft that fly the most
    *
    * @return Brand name with flights number where an aircraft of this brand has fly
    */
  def mostFlyableAircraftsBrands: Map[String, Int] = {
    queryToList(
      """
        SELECT brand, COUNT(Flight.id)
        FROM Flight JOIN Aircraft ON Flight.aircraft_id = Aircraft.id
        GROUP BY brand
      """,
      Utils.rsToTuple
    ).toMap
  }
  def avgAircraftSeats: Double = {
    query(
      "SELECT AVG(seats) FROM Aircraft",
      Utils.rsToDouble
    ).getOrElse(0)
  }

  def totalSeatsNumber(f: Flight): Option[Int] = {
    query(
      s"""
        SELECT seats FROM Flight
        JOIN Aircraft ON Flight.aircraft_id = Aircraft.id
        WHERE Flight.id = ${f.id}
      """,
      Utils.rsToInt
    )
  }
  def availableSeats(f: Flight): List[Int] = {
    def rsToTuple(rs: ResultSet): (Int, Int) = (rs.getInt(1), rs.getInt(2))
    val result = queryToList(
      s"""
        SELECT seat_number, seats FROM Booking
        JOIN Flight ON Flight.id = flight_id
        JOIN Aircraft ON Aircraft.id = aircraft_id
        WHERE flight_id = ${f.id}
        GROUP BY flight_id, client_id
        ORDER BY flight_id
      """,
      rsToTuple
    )
    val takenSeats = result.map(_._1).toSet
    val allSeats = (1 to result.map(_._2).head).toSet
    (allSeats diff takenSeats).toList.sorted
  }
  def avgFlightOccupancy(f: Flight): Option[Double] = {
    query(
      s"""
        SELECT COUNT(flight_id) / seats
        FROM Booking
        JOIN Flight ON Flight.id = flight_id
        JOIN Aircraft ON Flight.aircraft_id = Aircraft.id
        WHERE flight_id = ${f.id}
        GROUP BY flight_id
        ORDER BY flight_id
      """,
      Utils.rsToDouble
    )
  }
  def avgFlightsOccupancies: Map[Int, Double] = {
    def rsToTuple(rs: ResultSet): (Int, Double) =
      (rs.getInt(1), rs.getDouble(2))
    queryToList(
      s"""
        SELECT flight_id, COUNT(flight_id) / seats
        FROM Booking
        JOIN Flight ON Flight.id = flight_id
        JOIN Aircraft ON Flight.aircraft_id = Aircraft.id
        GROUP BY flight_id
        ORDER BY flight_id
      """,
      rsToTuple
    ).toMap
  }

  def airportsWithMostDepartures: Map[Airport, Int] = {
    queryToList(
      """
        SELECT departure_iata_code, name, COUNT(departure_iata_code)
        FROM Flight
        JOIN Airport ON Flight.departure_iata_code = Airport.iata_code
        GROUP BY departure_iata_code
      """,
      Utils.rsToAirportInt
    ).toMap
  }
  def airportsWithMostArrivals: Map[Airport, Int] = {
    queryToList(
      """
        SELECT arrival_iata_code, name, COUNT(arrival_iata_code)
        FROM Flight
        JOIN Airport ON Flight.arrival_iata_code = Airport.iata_code
        GROUP BY arrival_iata_code
      """,
      Utils.rsToAirportInt
    ).toMap
  }
}
