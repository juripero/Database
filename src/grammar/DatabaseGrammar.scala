package grammar
import scala.util.parsing.combinator._
import table_structure._
import common._
import java.text.SimpleDateFormat
import java.util.Date
import akka.actor._
import scala.concurrent.duration.Duration

object DatabaseGrammar extends RegexParsers { 
  
  
  var database = new TableCollection(Map())
  val handlerSystem = ActorSystem.create("TableHandlers")
  val commandSystem = ActorSystem.create("Commands")
  var handlers = Map[String, ActorRef]()
  
  def exit: 		Parser[Any] = """(?i)Exit""".r ^^ (x => Exit())
  def printcom: 	Parser[Any] = """(?i)Print""".r~>("""(?i)Dictionary""".r | tableName) ^^ (x => TelnetPrint(x.toString))
  def defineTable: 	Parser[Any] = """(?i)Define Table""".r~>tableName~"""(?i)Having Fields""".r~"("~extFieldList<~")" ^^ { case  x ~ _~_ ~ y => DefineTable(x, y)}
  def delete: 		Parser[Any] = """(?i)Delete""".r~>tableName~opt("""(?i)Where""".r~booleanExp) ^^ {case x ~ None => {commandSystem.actorOf(Props(new Delete(x, NullExp())))} case x ~ exp => commandSystem.actorOf(Props(new Delete(x, exp.get._2)))}
  def insert:		Parser[Any] = """(?i)Insert""".r~"("~>valueList~")"~"""(?i)Into""".r~tableName ^^ {case x ~_~_~ y => commandSystem.actorOf(Props(new Insert(y, x)))}
  
  def update:		Parser[Any]	= """(?i)Update""".r~>tableName~"""(?i)Set""".r~fieldName~"="~value~opt("""(?i)Where""".r~booleanExp) ^^ {
    case x ~_~ y ~_~ z ~ None => {commandSystem.actorOf(Props(Update(x, y, z, NullExp())))} case x ~_~ y ~_~ z ~ exp => commandSystem.actorOf(Props(Update(x, y, z, exp.get._2)))
  }
  
  def select:		Parser[View] = """(?i)Select""".r~>(tableName | queryList)~opt("""(?i)Where""".r~booleanExp) ^^ { case x ~ None => x match {
    case x:String => database.getTable(x).toView(NullExp()) case x:View => x.selectFromView(NullExp())
    }
  case x ~ exp => x match {case x:String => database.getTable(x).toView(exp.get._2) case x:View => x.selectFromView(exp.get._2)}}
  
 def project:		Parser[View] = """(?i)Project""".r~>(tableName | queryList)~"""(?i)Over""".r~fieldList ^^ { case x ~_~ flist => x match {
   case x:String => database.getTable(x).toView(NullExp()).asProjectedView(flist)
   case x:View => x.asProjectedView(flist)
   }
 }
  def join:			Parser[View]	= """(?i)Join""".r~>(tableName | queryList)~"""(?i)And""".r~(tableName | queryList) ^^ { case x ~_~ y => (x, y) match {
    case (x:String, y:String) => database.getTable(x).toView(NullExp()) join database.getTable(y).toView(NullExp())
    case (x:String, y:View) => database.getTable(x).toView(NullExp()) join y
    case (x:View, y:String) => x join database.getTable(y).toView(NullExp())
    case (x:View, y:View) => x join y
  }}
  
  def intersect: 	Parser[View] = """(?i)Intersect""".r~>(tableName | queryList)~"""(?i)And""".r~(tableName | queryList) ^^ { case x ~_~ y => (x, y) match {
    case (x:String, y:String) => database.getTable(x).toView(NullExp()) intersect database.getTable(y).toView(NullExp())
    case (x:String, y:View) => database.getTable(x).toView(NullExp()) intersect y
    case (x:View, y:String) => x intersect database.getTable(y).toView(NullExp())
    case (x:View, y:View) => x intersect y
  }}
  def union:		Parser[View] = """(?i)Union""".r~>(tableName | queryList)~"""(?i)And""".r~(tableName | queryList) ^^ { case x ~_~ y => (x, y) match {
    case (x:String, y:String) => database.getTable(x).toView(NullExp()) union database.getTable(y).toView(NullExp())
    case (x:String, y:View) => database.getTable(x).toView(NullExp()) union y
    case (x:View, y:String) => x union database.getTable(y).toView(NullExp())
    case (x:View, y:View) => x union y
  }}
  
  def minus:		Parser[View]	= """(?i)Minus""".r~>(tableName | queryList)~"""(?i)And""".r~(tableName | queryList) ^^ { case x ~_~ y => (x, y) match {
    case (x:String, y:String) => database.getTable(x).toView(NullExp()) intersect database.getTable(y).toView(NullExp())
    case (x:String, y:View) => database.getTable(x).toView(NullExp()) intersect y
    case (x:View, y:String) => x intersect database.getTable(y).toView(NullExp())
    case (x:View, y:View) => x intersect y
  }}
 
  def sort:			Parser[View] = """(?i)Order""".r~>(tableName | queryList)~"""(?i)By""".r~fieldName ^^ { case x ~_~ fname => x match {
    case x:String => database.getTable(x).toView(NullExp()).orderBy(fname)
    case x:View => x.orderBy(fname)
  }}
  
  def begin:		Parser[Any] = """(?i)Begin""".r ^^ (x => Begin(x))
  def end:			Parser[Any] = """(?i)End""".r ^^ (x => End(x))
  def pause:		Parser[Any] = """(?i)Pause""".r ^^ (x => Pause(x))
  
  def queryList: 	Parser[View] = queryStatement | "("~>queryStatement<~")"
  def queryStatement: Parser[View] = select | project | union | intersect | join | minus | sort
  
  def tableName: 	Parser[String] = """[a-zA-Z]+""".r
  def extFieldList: Parser[List[Field]] = repsep(fieldParam,",")
  def fieldParam:	Parser[Field] = fieldName<~"""(?i)Integer""".r ^^ (x => NumberField(x)) |
      fieldName<~"""(?i)Date""".r ^^ (x => DateField(x)) |
      fieldName<~"""(?i)Boolean""".r ^^ (x => BoolField(x)) | 
      fieldName<~"""(?i)Real""".r ^^ (x => RealField(x)) | 
      fieldName<~"""(?i)Varchar""".r ^^ (x => VarcharField(x))
  def fieldList: Parser[List[String]] = repsep(fieldName, ",")
  def fieldName:	Parser[String] = """[a-zA-Z]+""".r
  def value: 		Parser[DataType] = real | number | date | boolean | varchar
  def real:         Parser[DataType] = """\d+\.\d+""".r ^^ (x => RealType(x.toDouble))
  def number:		Parser[DataType] = """\d+""".r ^^ (x => NumberType(x.toInt))
  def date:			Parser[DataType] = "'"~>"""\d\d\/\d\d\/\d\d\d\d""".r<~"'" ^^ (x => DateType(new Date(new SimpleDateFormat("MM/dd/yyyy").parse(x.toString).getTime)))
  def boolean: 		Parser[DataType] = """(?i)true""".r ^^ (x => BoolType(true)) |
      """(?i)false""".r ^^ (x => BoolType(false))
  def varchar:		Parser[DataType] = "'"~>"""[[a-zA-z]+\s*]+""".r<~"'" ^^ (x => VarcharType(x.toString)) 
  def valueList:	Parser[List[DataType]] = repsep(value, ",")
  def relop:		Parser[String] = "<=" | ">=" | "=" | "!=" | "<" | ">"
  def booleanExp:	Parser[BooleanExpression] = fieldName~relop~value ^^ {case x ~ y => x._2 match {
    case "=" => new Equal(x._1, y)
    case "!=" => new NotEqual(x._1, y)
    case "<" => new LessThan(x._1, y)
    case ">" => new GreaterThan(x._1, y)
    case "<=" => new LTE(x._1, y)
    case ">=" => new GTE(x._1, y)
  }}
  def endLine: 		Parser[Any] = ";"
  def wrong: 		Parser[Any] = """(?i).*""".r ^^ (x => Wrong(x).execute)
  def command: 		Parser[Any] = (exit | printcom | defineTable | delete | insert | update | select | project | sort | join | union | intersect | minus)<~endLine | wrong
  
  def parse(input: String): Unit = 
    parseAll(command, input).get match {
    case x:Command => x.execute
    case v:View => v.printView
    }

  def parse2(input: String): ParseResult[Any] =
    parseAll(command, input)
    
    
  trait CommandActor extends Actor {
    import context._
    
    def execute: Any
    
     def waiting: Receive = {
      case Busy => {
        unbecome
        system.scheduler.scheduleOnce(Duration(20, "milliseconds"), self, Executing)
      }
    }
    
    def receive = {
      case Busy => {
        become(waiting)
        system.scheduler.scheduleOnce(Duration(20, "milliseconds"), self, Executing)
      }
      case Execute => {
        this.execute
      }
    }
  }
  
  
  abstract class Command {
    
    def execute: Any
    
  }
  
  /*
  abstract class Command {
    def execute: Any
  }*/
  case class Exit() extends Command {
    def execute: Unit = 
      println("~Bye~!")
      System.exit(1)
  }
  case class Print(com: String) extends Command {
    def execute: Unit = com match {
      case "dictionary" => database.printCollection
      case _ => {if (database.containsTable(com)) database.getTable(com).printTable else println("Table " + com + " not in database.")} 
    }
  }
  case class TelnetPrint(com: String) extends Command {
    override def execute: String = com match {
      case "dictionary" => database.socketPrintCollection
      case _ => {if (database.containsTable(com)) database.getTable(com).toString else "Table " + com + " not in database."} 
    }
  }
  case class DefineTable(name: String, fields: List[Field]) extends Command {
    def execute: Unit = {
      if(!database.containsTable(name)){
        database = database.addTable(new Table(name, fields, Nil))
        handlers = handlers + (name -> handlerSystem.actorOf(Props(new TableHandler(name))))
      }
      else
        println("Database already contains table " + name)
    }
  }
  case class Delete(tabName: String, exp: BooleanExpression) extends Command with CommandActor {
    def execute: Unit = {
      if (database.containsTable(tabName)) {
        handlers.get(tabName).get ! Executing
        database = database.addTable(database.getTable(tabName).delete(exp))
        handlers.get(tabName).get ! Done
      }
      else println("Table " + tabName + " not in database.")
    }
  }
  case class Insert(tabName: String, values: List[DataType]) extends Command with CommandActor {
    def execute: Unit = 
      if(database.containsTable(tabName)) {
        handlers.get(tabName).get ! Executing
        database = database.updateTable(tabName, database.getTable(tabName).insert(new Row(database.getTable(tabName).getFields.zip(values))))
        handlers.get(tabName).get ! Done
      }
      else
        println("Table " + tabName + " not in database.")
  }
  case class Update(tabName: String, fieldName: String, value: DataType, exp: BooleanExpression) extends Command with CommandActor {
    def execute: Unit = 
      if(database.containsTable(tabName)) {
        handlers.get(tabName).get ! Executing
        database = database.updateTable(tabName, database.getTable(tabName).update(fieldName, value, exp))
        handlers.get(tabName).get ! Done
      }
      else println("Table " + tabName + " not in database.")
  }
  case class Begin(com: Any) extends Command {
    def execute: Unit = 
      println(com)
  }
  case class End(com: Any) extends Command {
    def execute: Unit = 
      println(com)
  }
  case class Pause(com: Any) extends Command {
    def execute: Unit = 
      println(com)
  }
  case class Wrong(com: Any) extends Command {
    def execute: Unit = 
      println("You entered an incorrectly formatted command: ")
      println(com)
  }
}

