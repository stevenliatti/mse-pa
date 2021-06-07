package ch.hepia

import java.time.LocalDateTime

object FlyManagerSimulator extends App {
  val host = sys.env.get("MYSQL_HOST")
  val port = sys.env.getOrElse("MYSQL_PORT", "3306")
  val user = sys.env.get("MYSQL_USER")
  val password = sys.env.get("MYSQL_PASSWORD")
  val dbName = sys.env.get("MYSQL_DATABASE")
  val kHost = sys.env.get("KAFKA_HOST")
  val kPort = sys.env.get("KAFKA_PORT")

  val manager = (host, port, user, password, dbName, kHost, kPort) match {
    case (Some(h), po, Some(u), Some(pa), Some(d), Some(kh), Some(kp)) =>
      new MySQLDebeziumFlyManager(h, po, u, pa, d, kh, kp, "dbserver")
    case _ => sys.exit(42)
  }

  for {
    i <- 1 to 10
  } yield {
    println(
      s"$i, ${manager.avgFlightOccupancy(Flight(i, LocalDateTime.now, "asdf", "fdas", 2))}"
    )
  }

  println(manager.avgFlightsOccupancies.filter { case (k, v) => k < 11 })

  // val c1 = Client(1, "Bob", "Bobby", "bob@mail.com")
  // manager.updateClientFirstname(c1, c1.firstname)
  // Thread.sleep(2300)
  // manager.updateClientLastname(c1, c1.lastname)
  // Thread.sleep(1200)
  // manager.updateClientEmail(c1, c1.email)
  // manager.clientHistory(1).foreach(println)

  // val c2 = Client(2, "Fred", "Freddy", "fred@mail.com")
  // manager.updateClientFirstname(c2, c2.firstname)
  // Thread.sleep(1200)
  // manager.updateClientLastname(c2, c2.lastname)
  // Thread.sleep(2300)
  // manager.updateClientEmail(c2, c2.email)
  // manager.clientHistory(2).foreach(println)

  val c = manager.client(1).get._1
  val f = manager.flight(1).get._1
  val available = manager.availableSeats(f)
  println(available)
  if (available.nonEmpty) {
    val b = Booking(f.id, c.id, available.head, "pending")
    println(b)
    manager.createBooking(b)
  }
  println(manager.availableSeats(f))

  println(manager.client(c.id))
  println(manager.aircraft(1))
  println(manager.flight(f.id))
  println(manager.client(f.id, 1))
  println(manager.seatNumber(f.id, c.id))
  println(manager.avgAircraftSeats)
  println(manager.totalSeatsNumber(f))
  println(manager.avgFlightOccupancy(f))

}
