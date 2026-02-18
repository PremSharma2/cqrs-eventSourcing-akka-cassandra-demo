package playground

import actor.com.rockthejvm.bookings.actor.Hotel
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import model.{CancelReservation, MakeReservation}

import java.sql.Date
import scala.concurrent.duration._

object Playground {
  def main(args: Array[String]): Unit = {

    val simpleLogger =
      Behaviors
        .receive[Any] {
          (ctx, command) =>
            ctx.log.info(s"Logger Actor received Response  from Hotel Actor:->$command ")
            Behaviors.same
        }

    val rootGuardian =

      Behaviors
        .setup[String] {
        ctx =>
          ctx.log.info("Welcome to Akka!")
          val loggerActor = ctx.spawn(simpleLogger, "Logger-Actor")
          val hotelActor = ctx.spawn(Hotel("hotel-Actor-ID"), "Hotel-Actor")
//          hotelActor ! MakeReservation(
//            guestId = "Prem",
//            startDate = Date.valueOf("2022-07-14"),
//            endDate = Date.valueOf("2022-07-21"),
//            roomNumber = 101,
//            replyTo = loggerActor)

        hotelActor ! CancelReservation("NXI8EPYI8K",loggerActor)
          Behaviors.empty
      }

    val actorSystem = ActorSystem(rootGuardian, "Hotel-Demo")

    import actorSystem.executionContext
    actorSystem.scheduler.scheduleOnce(5.seconds, () => actorSystem.terminate())
    import scala.concurrent.Await
    import scala.concurrent.duration._

    Await.result(actorSystem.whenTerminated, 60.seconds)
  }
}