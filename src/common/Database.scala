package common

import akka.actor.{ Props, ActorSystem, Actor, ActorRef }
import TelnetServer.Response
import akka.actor.actorRef2Scala 

object Database {
  val system = ActorSystem.create("Database")
  def main(args: Array[String]) {
    val db = system.actorOf(Props(Database()), "Database")
    val server = system.actorOf(Props(new TelnetServer(db)), "Telnet")
  }
  case class Message(msg : String, user: ActorRef)
  case class ExitServer
  def apply() = new Database()
}


class Database extends Actor {
  import Database._
  import TelnetServer._
  import grammar.DatabaseGrammar._
  import table_structure.{View, Execute}
  import scala.io.Source

  def receive = {
    case Message(msg, user) =>
      if (msg == "exit;") ExitServer
      else if (msg.startsWith("exec")) {
        sender ! Response
        for (line <- Source.fromFile(System.getProperty("user.dir") + "/" + msg.substring(5, msg.length)).getLines) {
          sender ! PrintResponse(line)
          var result = parse2(line).get
          result match {
            case v: View => {
              sender ! PrintResponse(v.toString)
            }
            case x: Command => x.asInstanceOf[Command].execute match {
              case y: String => sender ! PrintResponse(y.toString)
              case _ => sender ! Response()
            }
            case a: ActorRef => {
              a ! Execute
              sender ! Response
            }
          }
        }
      }
      else {
        sender ! Response
        val result = parse2(msg).get
        result match {
          case v:View => {
            sender ! PrintResponse(v.toString)
          }
          case x:Command => x.asInstanceOf[Command].execute match {
            case y:String => sender ! PrintResponse(y.toString)
            case _ => sender ! Response
          }
          case a:ActorRef => {
            a ! Execute
            sender ! Response
          }
        }
      }
  }
}
