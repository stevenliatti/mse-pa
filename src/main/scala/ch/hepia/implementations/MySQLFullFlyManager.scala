/** MySQL only implementation
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import java.time.LocalDateTime

class MySQLFullFlyManager(
    host: String,
    port: String,
    user: String,
    password: String,
    dbName: String
) extends MySQLFlyManager(host, port, user, password, dbName) {

  def createClient(c: Client): Unit = {
    val nowStr = Utils.nowStr
    command(s"INSERT INTO Client VALUES (${c.id})")
    command(
      s"""
        INSERT INTO ClientFirstname
        VALUES (${c.id}, "$nowStr", "${c.firstname}")
      """
    )
    command(
      s"""
        INSERT INTO ClientLastname
        VALUES (${c.id}, "$nowStr", "${c.lastname}")
      """
    )
    command(
      s"""INSERT INTO ClientEmail VALUES (${c.id}, "$nowStr", "${c.email}")"""
    )
  }
  def createFlight(f: Flight): Unit = {
    val nowStr = Utils.nowStr
    val vs =
      s"""(
        ${f.id},
        "${f.departureIataCode}",
        "${f.arrivalIataCode}",
        ${f.aircraftId}
      )"""
    command(s"INSERT INTO Flight VALUES $vs")
    command(
      s"""
        INSERT INTO FlightDatetime
        VALUES (${f.id}, "$nowStr", "${f.datetime}")
      """
    )
  }
  def createBooking(b: Booking): Unit = {
    val nowStr = Utils.nowStr
    command(
      s"INSERT INTO Booking VALUES (${b.flightId}, ${b.clientId}, ${b.seat})"
    )
    command(
      s"""
        INSERT INTO BookingSeatNumber VALUES
        (${b.flightId}, ${b.clientId}, "$nowStr", ${b.seat})
      """
    )
    command(
      s"""
        INSERT INTO BookingState VALUES
        (${b.flightId}, ${b.clientId}, "$nowStr", "${b.state}")
      """
    )
  }

  def createClients(cs: List[Client]): Unit = {
    val nowStr = Utils.nowStr
    val vs = cs
      .map(c =>
        (
          s"(${c.id})",
          s"""(${c.id}, "$nowStr", "${c.firstname}")""",
          s"""(${c.id}, "$nowStr", "${c.lastname}")""",
          s"""(${c.id}, "$nowStr", "${c.email}")"""
        )
      )
    command(
      s"INSERT INTO Client VALUES ${vs.map(_._1).mkString(",")}"
    )
    command(
      s"INSERT INTO ClientFirstname VALUES ${vs.map(_._2).mkString(",")}"
    )
    command(
      s"INSERT INTO ClientLastname VALUES ${vs.map(_._3).mkString(",")}"
    )
    command(
      s"INSERT INTO ClientEmail VALUES ${vs.map(_._4).mkString(",")}"
    )
  }
  def createFlights(fs: List[Flight]): Unit = {
    val nowStr = Utils.nowStr
    val vs = fs
      .map(f =>
        (
          s"""(
            ${f.id},
            "${f.departureIataCode}",
            "${f.arrivalIataCode}",
            ${f.aircraftId}
          )""",
          s"""(${f.id}, "$nowStr", "${f.datetime}")"""
        )
      )
    command(
      s"INSERT INTO Flight VALUES ${vs.map(_._1).mkString(",")}"
    )
    command(
      s"INSERT INTO FlightDatetime VALUES ${vs.map(_._2).mkString(",")}"
    )
  }
  def createBookings(bs: List[Booking]): Unit = {
    val nowStr = Utils.nowStr
    val vs = bs
      .map(b =>
        (
          s"""(${b.flightId}, ${b.clientId}, ${b.seat})""",
          s"""(${b.flightId}, ${b.clientId}, "$nowStr", ${b.seat})""",
          s"""(${b.flightId}, ${b.clientId}, "$nowStr", "${b.state}")"""
        )
      )
    command(
      s"INSERT INTO Booking VALUES ${vs.map(_._1).mkString(",")}"
    )
    command(
      s"INSERT INTO BookingSeatNumber VALUES ${vs.map(_._2).mkString(",")}"
    )
    command(
      s"INSERT INTO BookingState VALUES ${vs.map(_._3).mkString(",")}"
    )
  }

  def updateBookingSeat(b: Booking, newSeatNumber: Int): Unit = {
    try {
      connection.setAutoCommit(false)
      connection.createStatement.executeUpdate(
        s"""
          UPDATE Booking SET seat_number = $newSeatNumber
          WHERE flight_id = ${b.flightId} AND client_id = ${b.clientId}
        """
      )
      connection.createStatement.executeUpdate(
        s"""
          INSERT INTO BookingSeatNumber VALUES
          (${b.flightId}, ${b.clientId}, "${Utils.nowStr}", $newSeatNumber)
        """
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
  }

  def updateBookingState(b: Booking, newState: String): Unit = {
    command(
      s"""INSERT INTO BookingState VALUES
      (${b.flightId}, ${b.clientId}, "${Utils.nowStr}", "$newState")"""
    )
  }
  def updateClientFirstname(c: Client, newFirstname: String): Unit = {
    command(
      s"""INSERT INTO ClientFirstname VALUES
      (${c.id}, "${Utils.nowStr}", "$newFirstname")"""
    )
  }
  def updateClientLastname(c: Client, newLastname: String): Unit = {
    command(
      s"""INSERT INTO ClientLastname VALUES
      (${c.id}, "${Utils.nowStr}", "$newLastname")"""
    )
  }
  def updateClientEmail(c: Client, newEmail: String): Unit = {
    command(
      s"""INSERT INTO ClientEmail VALUES
      (${c.id}, "${Utils.nowStr}", "$newEmail")"""
    )
  }
  def updateFlightDatetime(
      f: Flight,
      newDatetime: LocalDateTime
  ): Unit = {
    command(
      s"""INSERT INTO FlightDatetime VALUES
      (${f.id}, "${Utils.nowStr}", "$newDatetime")"""
    )
  }

  def deleteBooking(b: Booking): Unit = {
    try {
      connection.setAutoCommit(false)
      connection.createStatement.executeUpdate(
        s"""DELETE FROM BookingState
        WHERE booking_flight_id = ${b.flightId}
        AND booking_client_id = ${b.clientId}"""
      )
      connection.createStatement.executeUpdate(
        s"""DELETE FROM BookingSeatNumber
        WHERE booking_flight_id = ${b.flightId}
        AND booking_client_id = ${b.clientId}"""
      )
      connection.createStatement.executeUpdate(
        s"""DELETE FROM Booking
        WHERE flight_id = ${b.flightId}
        AND client_id = ${b.clientId}"""
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
  }
  def deleteFlight(f: Flight): Unit =
    try {
      connection.setAutoCommit(false)
      connection.createStatement.executeUpdate(
        s"DELETE FROM BookingState WHERE booking_flight_id = ${f.id}"
      )
      connection.createStatement.executeUpdate(
        s"DELETE FROM BookingSeatNumber WHERE booking_flight_id = ${f.id}"
      )
      connection.createStatement.executeUpdate(
        s"DELETE FROM Booking WHERE flight_id = ${f.id}"
      )
      connection.createStatement.executeUpdate(
        s"DELETE FROM FlightDatetime WHERE flight_id = ${f.id}"
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

  def client(id: Int): Option[(Client, Status)] = {
    query(
      s"""
        SELECT id,
        (
          SELECT firstname FROM ClientFirstname
          WHERE client_id = $id
          ORDER BY timestamp DESC LIMIT 1
        ) AS firstname,
        (
          SELECT lastname FROM ClientLastname
          WHERE client_id = $id
          ORDER BY timestamp DESC LIMIT 1
        ) AS lastname,
        (
          SELECT email FROM ClientEmail
          WHERE client_id = $id
          ORDER BY timestamp DESC LIMIT 1
        ) AS email
        FROM Client WHERE id = $id
      """,
      Utils.rsToClient
    ) match {
      case Some(v) => Some((v, PRESENT))
      case None    => None
    }
  }

  def flight(id: Int): Option[(Flight, Status)] = {
    query(
      s"""
        SELECT id,
        (
          SELECT datetime FROM FlightDatetime
          WHERE flight_id = $id
          ORDER BY timestamp DESC LIMIT 1
        ) AS datetime,
        departure_iata_code, arrival_iata_code, aircraft_id
        FROM Flight WHERE id = $id
      """,
      Utils.rsToFlight
    ) match {
      case Some(v) => Some((v, PRESENT))
      case None    => None
    }
  }
  def client(
      flightId: Int,
      seatNumber: Int
  ): Option[(Client, Status)] = {
    query(
      s"""
        SELECT client_id FROM Booking
        WHERE flight_id = $flightId AND seat_number = $seatNumber
      """,
      Utils.rsToInt
    ) match {
      case Some(i) => client(i)
      case None    => None
    }
  }
  def seatNumber(flightId: Int, clientId: Int): Option[(Int, Status)] = {
    query(
      s"""
        SELECT seat_number FROM Booking
        WHERE flight_id = $flightId AND client_id = $clientId
      """,
      Utils.rsToInt
    ) match {
      case Some(v) => Some((v, PRESENT))
      case None    => None
    }
  }

  def clients: List[Client] = {
    queryToList(
      """
        SELECT id, firstname, lastname, email FROM (
          SELECT ROW_NUMBER() OVER (
            ORDER BY id,
              ClientFirstname.timestamp DESC,
              ClientLastname.timestamp DESC,
              ClientEmail.timestamp DESC
          ) AS virtual_id, id, firstname, lastname, email
          FROM Client AS C
          JOIN ClientFirstname ON C.id = ClientFirstname.client_id
          JOIN ClientLastname ON C.id = ClientLastname.client_id
          JOIN ClientEmail ON C.id = ClientEmail.client_id
        ) AS sorted_timestamps
        GROUP BY sorted_timestamps.id
        ORDER BY sorted_timestamps.id
      """,
      Utils.rsToClient
    )
  }
  def flights: List[Flight] = {
    queryToList(
      """
        SELECT
          id, datetime, departure_iata_code, arrival_iata_code, aircraft_id
        FROM (
          SELECT ROW_NUMBER() OVER (
            ORDER BY flight_id, timestamp DESC
          ) AS virtual_id, flight_id, datetime
          FROM FlightDatetime
        ) AS sorted_timestamps
        JOIN Flight ON Flight.id = sorted_timestamps.flight_id
        GROUP BY sorted_timestamps.flight_id
        ORDER BY sorted_timestamps.flight_id
      """,
      Utils.rsToFlight
    )
  }

  def clientsEmail: List[String] = {
    queryToList(
      """
        SELECT email FROM (
          SELECT ROW_NUMBER() OVER (
            ORDER BY client_id, timestamp DESC
          ) AS virtual_id, client_id, email
          FROM ClientEmail
        ) AS sorted_timestamps
        GROUP BY sorted_timestamps.client_id
        ORDER BY sorted_timestamps.client_id
      """,
      Utils.rsToString
    )
  }

  def clientFlights(c: Client): List[Flight] = {
    queryToList(
      s"""
        SELECT id, datetime, departure_iata_code, arrival_iata_code, aircraft_id
        FROM (
          SELECT ROW_NUMBER() OVER (
            ORDER BY flight_id, timestamp DESC
          ) AS virtual_id, flight_id, datetime
          FROM FlightDatetime
        ) AS sorted_timestamps
        JOIN Flight ON Flight.id = sorted_timestamps.flight_id
        JOIN Booking ON Booking.flight_id = Flight.id
        WHERE Booking.client_id = ${c.id}
        GROUP BY sorted_timestamps.flight_id
        ORDER BY sorted_timestamps.flight_id
      """,
      Utils.rsToFlight
    )
  }

  def clientHistory(clientId: Int): (List[ClientHistory], Status) = {
    queryToList(
      s"""
        SELECT
          id, firstname, lastname, email, ClientFirstname.timestamp,
          ClientLastname.timestamp, ClientEmail.timestamp
        FROM Client AS C
        JOIN ClientFirstname ON C.id = ClientFirstname.client_id
        JOIN ClientLastname ON C.id = ClientLastname.client_id
        JOIN ClientEmail ON C.id = ClientEmail.client_id
        WHERE id = $clientId
        ORDER BY
          ClientFirstname.timestamp DESC,
          ClientLastname.timestamp DESC,
          ClientEmail.timestamp DESC
      """,
      Utils.rsToClientHistory
    ) match {
      case Nil => (Nil, UNKNOWN)
      case l   => (l, PRESENT)
    }
  }
  def clientsHistory: Map[Int, (List[ClientHistory], Status)] = {
    queryToList(
      s"""
          SELECT
            id, firstname, lastname, email, ClientFirstname.timestamp,
            ClientLastname.timestamp, ClientEmail.timestamp
          FROM Client AS C
          JOIN ClientFirstname ON C.id = ClientFirstname.client_id
          JOIN ClientLastname ON C.id = ClientLastname.client_id
          JOIN ClientEmail ON C.id = ClientEmail.client_id
        """,
      Utils.rsToClientHistory
    ).groupBy(_.id).map { case (k, v) => (k, (v, PRESENT)) }
  }
  def bookingHistory(
      flightId: Int,
      clientId: Int
  ): (List[BookingHistory], Status) = {
    queryToList(
      s"""
        SELECT
          flight_id, client_id, bsn.seat_number, bst.state,
          bsn.timestamp AS bsnts, bst.timestamp AS bstts
        FROM Booking AS b
        JOIN BookingSeatNumber AS bsn ON bsn.booking_flight_id = b.flight_id
          AND bsn.booking_client_id = b.client_id
        JOIN BookingState AS bst ON bst.booking_flight_id = b.flight_id
          AND bst.booking_client_id = b.client_id
        WHERE flight_id = $flightId AND client_id = $clientId
        ORDER BY bsnts DESC, bstts DESC
      """,
      Utils.rsToBookingHistory
    ) match {
      case Nil => (Nil, UNKNOWN)
      case l   => (l, PRESENT)
    }
  }
  def flightHistory(flightId: Int): (List[FlightHistory], Status) = {
    queryToList(
      s"""
        SELECT
          id, datetime, departure_iata_code,
          arrival_iata_code, aircraft_id, timestamp
        FROM Flight AS f
        JOIN FlightDatetime AS fd ON f.id = fd.flight_id
        WHERE flight_id = $flightId
        ORDER BY timestamp DESC
      """,
      Utils.rsToFlightHistory
    ) match {
      case Nil => (Nil, UNKNOWN)
      case l   => (l, PRESENT)
    }
  }
}
