/** FlyManager trait, implementations contract.
  *
  * Define all functionalities that a FlyManager had to implement.
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import java.time.LocalDateTime

trait FlyManager {

  // Commands
  def createAirport(a: Airport): Unit
  def createAircraft(a: Aircraft): Unit
  def createClient(c: Client): Unit
  def createFlight(f: Flight): Unit
  def createBooking(b: Booking): Unit

  def createAirports(as: List[Airport]): Unit
  def createAircrafts(as: List[Aircraft]): Unit
  def createClients(cs: List[Client]): Unit
  def createFlights(fs: List[Flight]): Unit
  def createBookings(bs: List[Booking]): Unit

  def updateBookingSeat(b: Booking, newSeatNumber: Int): Unit
  def updateBookingState(b: Booking, newState: String): Unit
  def updateClientFirstname(c: Client, newFirstname: String): Unit
  def updateClientLastname(c: Client, newLastname: String): Unit
  def updateClientEmail(c: Client, newEmail: String): Unit
  def updateFlightDatetime(f: Flight, newDatetime: LocalDateTime): Unit

  def deleteBooking(b: Booking): Unit
  def deleteFlight(f: Flight): Unit

  // Queries
  def client(id: Int): Option[(Client, Status)]
  def aircraft(id: Int): Option[Aircraft]
  def flight(id: Int): Option[(Flight, Status)]

  def client(flightId: Int, seatNumber: Int): Option[(Client, Status)]
  def seatNumber(flightId: Int, clientId: Int): Option[(Int, Status)]

  def clients: List[Client]
  def flights: List[Flight]
  def airports: List[Airport]
  def aircrafts: List[Aircraft]

  def aircraftsBrands: List[String]

  /** Returns the brands of aircraft that fly the most
    *
    * @return Brand name with flights number where an aircraft of this brand has fly
    */
  def mostFlyableAircraftsBrands: Map[String, Int]
  def avgAircraftSeats: Double

  def clientsEmail: List[String]
  def totalSeatsNumber(f: Flight): Option[Int]
  def availableSeats(f: Flight): List[Int]
  def avgFlightOccupancy(f: Flight): Option[Double]
  def avgFlightsOccupancies: Map[Int, Double]
  def clientFlights(c: Client): List[Flight]
  def airportsWithMostDepartures: Map[Airport, Int]
  def airportsWithMostArrivals: Map[Airport, Int]

  def clientHistory(clientId: Int): (List[ClientHistory], Status)
  def clientsHistory: Map[Int, (List[ClientHistory], Status)]
  def bookingHistory(
      flightId: Int,
      clientId: Int
  ): (List[BookingHistory], Status)
  def flightHistory(flightId: Int): (List[FlightHistory], Status)
}
