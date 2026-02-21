package apps



import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.PersistenceQuery
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSession, CassandraSessionRegistry}
import akka.stream.scaladsl.{Sink, Source}
import model.{Reservation, ReservationAccepted, ReservationCanceled, ReservationUpdated}

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.concurrent.{ExecutionContext, Future}

// Query side from CQRS
// Reads events (Akka Persistence Query) and updates read tables in Cassandra
object HotelEventReader {

  implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "HotelEventReaderSystem")
  implicit val ec: ExecutionContext = system.executionContext

  // read journal
  private val readJournal: CassandraReadJournal =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  // Cassandra session
  val session: CassandraSession =
    CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")

  // Stream of all persistence IDs (not used in main, but kept)
  val persistenceIds: Source[String, NotUsed] = readJournal.persistenceIds()
  val consumptionSink = Sink.foreach(println)
  //link Source and Sink
  val connectedGraph = persistenceIds.to(consumptionSink)

  // ---------------------------
  // CQL (prepared-style strings)
  // ---------------------------
  private val GuestLastName = "Prem-Kaushik"

  private val UpdateAvailabilityCql =
    """
      |UPDATE hotel.available_rooms_by_hotel_date
      |SET is_available = ?
      |WHERE hotel_id = ? AND date = ? AND room_number = ?
      |""".stripMargin.trim

  private val InsertByHotelDateCql =
    """
      |INSERT INTO reservation.reservations_by_hotel_date
      |(hotel_id, start_date, end_date, room_number, confirm_number, guest_id)
      |VALUES (?, ?, ?, ?, ?, ?)
      |""".stripMargin.trim

  private val InsertByGuestCql =
    """
      |INSERT INTO reservation.reservations_by_guest
      |(guest_last_name, hotel_id, start_date, end_date, room_number, confirm_number, guest_id)
      |VALUES (?, ?, ?, ?, ?, ?, ?)
      |""".stripMargin.trim

  private val DeleteByHotelDateCql =
    """
      |DELETE FROM reservation.reservations_by_hotel_date
      |WHERE hotel_id = ? AND start_date = ? AND room_number = ?
      |""".stripMargin.trim

  private val DeleteByGuestCql =
    """
      |DELETE FROM reservation.reservations_by_guest
      |WHERE guest_last_name = ? AND confirm_number = ?
      |""".stripMargin.trim

  // ---------------------------
  // helpers (small + reusable)
  // ---------------------------
  private def datesInRange(start: LocalDate, end: LocalDate): Iterator[LocalDate] = {
    val nDays = start.until(end, ChronoUnit.DAYS).toInt // end-exclusive (same as your logic)
    (0L until nDays.toLong).iterator.map(start.plusDays)
  }

  private def swallowDone(label: String)(f: Future[Done]): Future[Done] =
    f.recover { case e =>
      println(s"$label failed: $e")
      Done
    }

  private def writeAll(fs: List[Future[Done]]): Future[Unit] =
    Future.sequence(fs).map(_ => ())

  import java.sql.Date

  private def setAvailability(
                               hotelId: String,
                               day: LocalDate,
                               roomNumber: Int,
                               isAvailable: Boolean
                             ): Future[Done] =
    swallowDone("Room day availability update") {
      session.executeWrite(
        UpdateAvailabilityCql,
        java.lang.Boolean.valueOf(isAvailable),
        hotelId,
        Date.valueOf(day),
        java.lang.Integer.valueOf(roomNumber)
      )
    }


  // ---------------------------
  // write-model -> read-model
  // ---------------------------
  private def makeReservation(reservation: Reservation): Future[Unit] = {
    val Reservation(guestId, hotelId, startDate, endDate, roomNumber, confirmationNumber) = reservation

    val startLocalDate = startDate.toLocalDate
    val endLocalDate   = endDate.toLocalDate

    val blockDays: List[Future[Done]] = {
      //creating the date range Window so that we can block the window
      //So that Room day availability updated and blocked for this window  bookings
      datesInRange(startLocalDate, endLocalDate)
        .map(day => setAvailability(hotelId, day, roomNumber, isAvailable = false))
        .toList
    }

    //populating the hotel reservations table so that we can see which hotel is
    //booked for that window or any kind of analysis we do
    val byHotelDate: Future[Done] =
      swallowDone("Insert reservation by hotel/date") {
        session.executeWrite(
          InsertByHotelDateCql,
          hotelId,
          startDate,
          endDate,
          java.lang.Integer.valueOf(roomNumber),
          confirmationNumber,
          guestId
        )
      }

    //Populating guest bookings Table So that we do
    // analytics like Most preferred Hotel by guests
    val byGuest: Future[Done] =
      swallowDone("Insert reservation by guest") {
        session.executeWrite(
          InsertByGuestCql,
          GuestLastName,
          hotelId,
          startDate,
          endDate,
          java.lang.Integer.valueOf(roomNumber),
          confirmationNumber,
          guestId
        )
      }

    writeAll(byGuest :: byHotelDate :: blockDays)
  }

  def removeReservation(reservation: Reservation): Future[Unit] = {
    val Reservation(_, hotelId, startDate, endDate, roomNumber, confirmationNumber) = reservation

    val startLocalDate = startDate.toLocalDate
    val endLocalDate   = endDate.toLocalDate

    val unblockDays: List[Future[Done]] =
      datesInRange(startLocalDate, endLocalDate)
        .map(day => setAvailability(hotelId, day, roomNumber, isAvailable = true))
        .toList

    val removeByHotelDate: Future[Done] =
      swallowDone("Delete reservation by hotel/date") {
        session.executeWrite(DeleteByHotelDateCql, hotelId, startDate,  Integer.valueOf(roomNumber))
      }

    val removeByGuest: Future[Done] =
      swallowDone("Delete reservation by guest") {
        session.executeWrite(DeleteByGuestCql, GuestLastName, confirmationNumber)
      }

    writeAll(removeByGuest :: removeByHotelDate :: unblockDays)
  }

  // all events for a persistence ID
  val eventsForTestHotel =
    readJournal
      .eventsByPersistenceId("hotel_82", 0L, Long.MaxValue)
      .map(_.event)
      .mapAsync(8) {
        case ReservationAccepted(res) =>
          println(s"MAKING RESERVATION: $res")
          makeReservation(res)

        case ReservationUpdated(oldReservation, newReservation) =>
          println(s"CHANGING RESERVATION: from $oldReservation to $newReservation")
          removeReservation(oldReservation).flatMap(_ => makeReservation(newReservation))

        case ReservationCanceled(res) =>
          println(s"CANCELLING RESERVATION: $res")
          removeReservation(res)

        case other =>
          // keep stream safe if you add new events later
          println(s"IGNORING EVENT: $other")
          Future.successful(())
      }

  def main(args: Array[String]): Unit = {
    eventsForTestHotel.runWith(Sink.ignore)
  }
}

