/** Domain / model for all entities.
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import java.time.LocalDateTime

trait Fly
case class Airport(iataCode: String, name: String) extends Fly
case class Aircraft(id: Int, brand: String, seats: Int) extends Fly
case class Client(id: Int, firstname: String, lastname: String, email: String)
    extends Fly
case class Flight(
    id: Int,
    datetime: LocalDateTime,
    departureIataCode: String,
    arrivalIataCode: String,
    aircraftId: Int
) extends Fly {
  require(departureIataCode != arrivalIataCode)
}
case class Booking(flightId: Int, clientId: Int, seat: Int, state: String)
    extends Fly

case class ClientHistory(
    id: Int,
    firstname: String,
    lastname: String,
    email: String,
    tsFirstname: LocalDateTime,
    tsLastname: LocalDateTime,
    tsEmail: LocalDateTime
) extends Fly {
  def toClient: Client = Client(id, firstname, lastname, email)
}
case class BookingHistory(
    flightId: Int,
    clientId: Int,
    seat: Int,
    state: String,
    tsSeat: LocalDateTime,
    tsState: LocalDateTime
) extends Fly {
  def toBooking: Booking = Booking(flightId, clientId, seat, state)
}
case class FlightHistory(
    id: Int,
    datetime: LocalDateTime,
    departureIataCode: String,
    arrivalIataCode: String,
    aircraftId: Int,
    tsDatetime: LocalDateTime
) extends Fly {
  require(departureIataCode != arrivalIataCode)
  def toFlight: Flight =
    Flight(id, datetime, departureIataCode, arrivalIataCode, aircraftId)
}

sealed trait Operation
case object Create extends Operation
case object Update extends Operation
case object Delete extends Operation

trait FlyEvent

case class AircraftEvent(
    before: Option[Aircraft],
    after: Option[Aircraft],
    op: Operation,
    timestamp: LocalDateTime
) extends FlyEvent
case class AirportEvent(
    before: Option[Airport],
    after: Option[Airport],
    op: Operation,
    timestamp: LocalDateTime
) extends FlyEvent
case class BookingEvent(
    before: Option[Booking],
    after: Option[Booking],
    op: Operation,
    timestamp: LocalDateTime
) extends FlyEvent
case class ClientEvent(
    before: Option[Client],
    after: Option[Client],
    op: Operation,
    timestamp: LocalDateTime
) extends FlyEvent
case class FlightEvent(
    before: Option[Flight],
    after: Option[Flight],
    op: Operation,
    timestamp: LocalDateTime
) extends FlyEvent
