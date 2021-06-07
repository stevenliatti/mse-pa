/** Debezium deserialier
  *
  * Offer eventToFlyModelEvent function who return an Option
  * of data class from Domain, from a given key and value.
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

import java.time.Instant
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.TimeZone

object FlyDebeziumDeserializer {
  import spray.json._
  import DefaultJsonProtocol._

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def eventToFlyModelEvent(key: JsValue, value: JsValue): Option[FlyEvent] = {
    val keyType = readKeyType(key)
    if (keyType.isDefined) {
      keyType.get match {
        case "Aircraft" => Some(readAircraftEvent(value))
        case "Airport"  => Some(readAirportEvent(value))
        case "Booking"  => Some(readBookingEvent(value))
        case "Client"   => Some(readClientEvent(value))
        case "Flight"   => Some(readFlightEvent(value))
      }
    } else None
  }

  private def readKeyType(value: JsValue): Option[String] = value match {
    case JsObject(fields) =>
      fields.get("schema") match {
        case Some(schema) =>
          schema match {
            case JsObject(schemaFields) =>
              schemaFields.get("name") match {
                case Some(name) =>
                  name match {
                    case JsString(value) => {
                      val tokens = value.split("\\.")
                      if (tokens.nonEmpty) Some(tokens(2))
                      else None
                    }
                    case _ => None
                  }
                case _ => None
              }
            case _ => None
          }
        case _ => None
      }
    case _ => None
  }

  private def strOpToOperation(op: String): Operation = op match {
    case "c" => Create
    case "u" => Update
    case "d" => Delete
  }

  private def payload(value: JsValue): (
      Option[Map[String, JsValue]],
      Option[Map[String, JsValue]],
      Operation,
      LocalDateTime
  ) = {
    val payload = value.asJsObject
      .fields("payload")
      .asJsObject

    val timestamp = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(payload.fields("ts_ms").convertTo[Long]),
      TimeZone.getDefault.toZoneId
    )
    val op = strOpToOperation(payload.fields("op").convertTo[String])
    val after = op match {
      case Create | Update => Some(payload.fields("after").asJsObject.fields)
      case _               => None
    }
    val before = op match {
      case Delete | Update => Some(payload.fields("before").asJsObject.fields)
      case _               => None
    }

    (before, after, op, timestamp)
  }

  private def readAircraftEvent(value: JsValue): AircraftEvent = {
    def eventToAircraft(ba: Option[Map[String, JsValue]]): Option[Aircraft] =
      ba match {
        case Some(fields) => {
          val id = fields("id").convertTo[Int]
          val brand = fields("brand").convertTo[String]
          val seats = fields("seats").convertTo[Int]
          Some(Aircraft(id, brand, seats))
        }
        case None => None
      }

    val (before, after, op, timestamp) = payload(value)
    AircraftEvent(
      eventToAircraft(before),
      eventToAircraft(after),
      op,
      timestamp
    )
  }

  private def readAirportEvent(value: JsValue): AirportEvent = {
    def eventToAirport(ba: Option[Map[String, JsValue]]): Option[Airport] =
      ba match {
        case Some(fields) => {
          val iataCode = fields("iata_code").convertTo[String]
          val name = fields("name").convertTo[String]
          Some(Airport(iataCode, name))
        }
        case None => None
      }

    val (before, after, op, timestamp) = payload(value)
    AirportEvent(
      eventToAirport(before),
      eventToAirport(after),
      op,
      timestamp
    )
  }

  private def readBookingEvent(value: JsValue): BookingEvent = {
    def eventToBooking(ba: Option[Map[String, JsValue]]): Option[Booking] =
      ba match {
        case Some(fields) => {
          val flightId = fields("flight_id").convertTo[Int]
          val clientId = fields("client_id").convertTo[Int]
          val seat = fields("seat_number").convertTo[Int]
          val state = fields("state").convertTo[String]
          Some(Booking(flightId, clientId, seat, state))
        }
        case None => None
      }

    val (before, after, op, timestamp) = payload(value)
    BookingEvent(
      eventToBooking(before),
      eventToBooking(after),
      op,
      timestamp
    )
  }

  private def readClientEvent(value: JsValue): ClientEvent = {
    def eventToClient(ba: Option[Map[String, JsValue]]): Option[Client] =
      ba match {
        case Some(fields) => {
          val id = fields("id").convertTo[Int]
          val firstname = fields("firstname").convertTo[String]
          val lastname = fields("lastname").convertTo[String]
          val email = fields("email").convertTo[String]
          Some(Client(id, firstname, lastname, email))
        }
        case None => None
      }

    val (before, after, op, timestamp) = payload(value)
    ClientEvent(
      eventToClient(before),
      eventToClient(after),
      op,
      timestamp
    )
  }

  private def readFlightEvent(value: JsValue): FlightEvent = {
    def eventToFlight(ba: Option[Map[String, JsValue]]): Option[Flight] =
      ba match {
        case Some(fields) => {
          val id = fields("id").convertTo[Int]
          val datetime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(fields("datetime").convertTo[Long]),
            TimeZone.getDefault.toZoneId
          )
          val departureIataCode =
            fields("departure_iata_code").convertTo[String]
          val arrivalIataCode = fields("arrival_iata_code").convertTo[String]
          val aircraftId = fields("aircraft_id").convertTo[Int]
          Some(
            Flight(id, datetime, departureIataCode, arrivalIataCode, aircraftId)
          )
        }
        case None => None
      }

    val (before, after, op, timestamp) = payload(value)
    FlightEvent(
      eventToFlight(before),
      eventToFlight(after),
      op,
      timestamp
    )
  }
}
