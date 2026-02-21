package apps


import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.{Sink, Source}

import java.time.LocalDate
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Todo
 * Read side (Query side):
 * Cassandra tables are shaped for fast reads, like:
 * “show me available rooms for hotel X on date D”
 * “show me reservations by guest”
 * “show me reservations by hotel-date”
 * This File will set up the DB for CQRS to use this Warehouse for all All hotels
 * this is single source of truth This Q of CQRS is Consumer of Events
 * which are stream through kafka into Read side
 */

object CassandraTableCleaner {

  implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "CassandraSystem")

  implicit val ec: ExecutionContext =
    system.executionContext

  // IMPORTANT: must match config path used by sessionFor(...)
  private val session =
    CassandraSessionRegistry(system)
      .sessionFor("akka.projection.cassandra.session-config")

  // Domain constants
  private val StartDate = LocalDate.of(2023, 1, 1)
  private val NumHotels = 100
  private val RoomsPerHotel = 100
  private val Days = 365

  private val TruncateStatements: List[String] = List(
    "TRUNCATE akka.all_persistence_ids",
    "TRUNCATE akka.messages",
    "TRUNCATE akka.metadata",
    "TRUNCATE akka.tag_scanning",
    "TRUNCATE akka.tag_views",
    "TRUNCATE akka.tag_write_progress",
    "TRUNCATE akka_snapshot.snapshots",
    "TRUNCATE reservation.reservations_by_hotel_date",
    "TRUNCATE reservation.reservations_by_guest",
    "TRUNCATE hotel.available_rooms_by_hotel_date"
  )

  private val InsertAvailabilityCql: String =
    """
      |INSERT INTO hotel.available_rooms_by_hotel_date
      |(hotel_id, date, room_number, is_available)
      |VALUES (?, ?, ?, ?)
      |""".stripMargin.trim


  /**
   * TRUNCATE is DDL in Cassandra driver terminology.
   * executeDDL is the right API for it (executeWrite sometimes works but is not semantically correct).
   */
  private def truncateAll(): Future[Done] =
    Future
      .sequence(TruncateStatements.map(stmt => session.executeDDL(stmt)))
      .map(_ => Done)

  private def availabilityRows: Iterator[(String, LocalDate, Int, Boolean)] =
    for {
      i <- (1 to NumHotels).iterator
      hotelId = s"hotel_$i"
      roomNumber <- (1 to RoomsPerHotel).iterator
      dayOffset <- (0 until Days).iterator
      day = StartDate.plusDays(dayOffset.toLong)
    } yield (hotelId, day, roomNumber, true)

  private def populateAvailability(parallelism: Int = 8): Future[Done] =
    Source
      .fromIterator(() => availabilityRows)
      .mapAsync(parallelism) {
        case (hotelId, day, roomNumber, isAvailable) =>
          session
            .executeWrite(
              InsertAvailabilityCql,
              hotelId,
              day,
              Int.box(roomNumber),
              Boolean.box(isAvailable)
            )
      }
      .runWith(Sink.ignore)
      .map(_ => Done)

  private def run(): Future[Done] =
    for {
      //_ <- waitForCassandra()               // <-- critical fix
      _ <- truncateAll()
      _ <- populateAvailability(parallelism = 8)
    } yield Done

  def main(args: Array[String]): Unit = {
    run().onComplete {
      case Success(_) =>
        system.log.info("All tables cleared and availability repopulated.")
        system.terminate()

      case Failure(ex) =>
        system.log.error("Failed during Cassandra cleanup.", ex)
        system.terminate()
    }
  }
}

