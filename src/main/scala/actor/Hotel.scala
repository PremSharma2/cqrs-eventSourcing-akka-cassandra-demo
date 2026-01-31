package actor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import model.{Command, Event, Reservation}

// primary persistent actor
object Hotel {
//state of Hotel Actor
  private case class State(reservations: Set[Reservation])
  
  //command handler
 private def commandHandler(hotelId: String): (State, Command) => Effect[Event, State]= ???

  //event handler
  private  def eventHandler(hotelId: String): (State, Event) => State = ???
  def apply(hotelId:String): Behavior[Command]  =
    EventSourcedBehavior[Command, Event, State](
      persistenceId = PersistenceId.ofUniqueId(hotelId),
      emptyState = State(Set()),
      commandHandler = commandHandler(hotelId),
      eventHandler = eventHandler(hotelId)
    )


}
