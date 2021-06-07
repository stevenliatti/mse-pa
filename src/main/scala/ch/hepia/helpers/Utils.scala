/** Utils functions
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import java.io._
import java.sql.ResultSet
import java.time.LocalDateTime
import scala.util.Random

/** Some utils functions
  */
object Utils {
  import scala.io.Source

  /** Read file and return it as lines
    *
    * @param src
    * @return
    */
  def readLinesFromFile(src: String) = Source.fromFile(src).getLines().toList

  /** Write a Seq[String] to the filename.
    *
    * @param filename
    * @param lines
    */
  def writeToFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(s"$line\n")
    }
    bw.close()
  }

  def randBetween(from: Int, to: Int): Int =
    from + Random.nextInt(to + 1 - from)

  def dateTimeToSQL(d: LocalDateTime): String =
    s"${d.toLocalDate} ${d.toLocalTime}"

  def nowStr: String = dateTimeToSQL(LocalDateTime.now)

  def sqlDatetimeToLocalDateTime(rs: ResultSet, colIdx: Int): LocalDateTime =
    LocalDateTime.of(
      rs.getDate(colIdx).toLocalDate,
      rs.getTime(colIdx).toLocalTime
    )

  def rsToClient(rs: ResultSet): Client =
    Client(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4))
  def rsToAircraft(rs: ResultSet): Aircraft =
    Aircraft(rs.getInt(1), rs.getString(2), rs.getInt(3))
  def rsToAirport(rs: ResultSet): Airport =
    Airport(rs.getString(1), rs.getString(2))
  def rsToBooking(rs: ResultSet): Booking =
    Booking(
      rs.getInt(1),
      rs.getInt(2),
      rs.getInt(3),
      rs.getString(4)
    )
  def rsToFlight(rs: ResultSet): Flight =
    Flight(
      rs.getInt(1),
      sqlDatetimeToLocalDateTime(rs, 2),
      rs.getString(3),
      rs.getString(4),
      rs.getInt(5)
    )
  def rsToInt(rs: ResultSet): Int = rs.getInt(1)
  def rsToDouble(rs: ResultSet): Double = rs.getDouble(1)
  def rsToString(rs: ResultSet): String = rs.getString(1)
  def rsToTuple(rs: ResultSet): (String, Int) =
    (rs.getString(1), rs.getInt(2))
  def rsToAirportInt(rs: ResultSet): (Airport, Int) =
    (Airport(rs.getString(1), rs.getString(2)), rs.getInt(3))

  def rsToClientHistory(rs: ResultSet): ClientHistory = {
    ClientHistory(
      rs.getInt(1),
      rs.getString(2),
      rs.getString(3),
      rs.getString(4),
      sqlDatetimeToLocalDateTime(rs, 5),
      sqlDatetimeToLocalDateTime(rs, 6),
      sqlDatetimeToLocalDateTime(rs, 7)
    )
  }
  def rsToBookingHistory(rs: ResultSet): BookingHistory = {
    BookingHistory(
      rs.getInt(1),
      rs.getInt(2),
      rs.getInt(3),
      rs.getString(4),
      sqlDatetimeToLocalDateTime(rs, 5),
      sqlDatetimeToLocalDateTime(rs, 6)
    )
  }
  def rsToFlightHistory(rs: ResultSet): FlightHistory = {
    FlightHistory(
      rs.getInt(1),
      sqlDatetimeToLocalDateTime(rs, 2),
      rs.getString(3),
      rs.getString(4),
      rs.getInt(5),
      sqlDatetimeToLocalDateTime(rs, 6)
    )
  }
}
