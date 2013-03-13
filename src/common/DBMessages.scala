package common

trait DBMessages {
  import akka.actor.{ActorRef}
  
  abstract class DBMessages
  case class Execute(user: ActorRef)
  case class Occupied

}