package table_structure
import akka.actor.{Actor, ActorRef}
import akka.util._


abstract class Messages
case class Execute extends Messages
case class Executing extends Messages
case class Busy extends Messages
case class Done extends Messages


class TableHandler(tabName: String) extends Actor {
  import context._
  
  def working(user: ActorRef): Receive = {
    case Executing => sender ! Busy
    case Done => unbecome
  }
  
  def receive = {
    case Executing => {
      become(working(sender))
    }
  }
}

class Table(name: String, fieldList: List[Field], rows: List[Row]) {

  def getTableName: String =
    name

  def getFields: List[Field] =
    fieldList

  def getField(fieldName: String): Field =
    fieldList.find(elem => elem.getFieldName == fieldName).get

  def printTable: Unit = {
    println("Name: " + name)
    println("Fields: ")
    println(fieldList.foreach(field => print(field.toString + " | ")))
    rows.foreach(row => println(row.printRow))
  }
  
  def printTabInfo: Unit = {
    println("Name: " + name)
    println("Fields: ")
    println(fieldList.foreach(field => print(field.toString + " | ")))
  }
  
  def stringTabInfo: String = {
    "Name: " + name + "\n" +
    "Fields: " + (fieldList.map(field => field.toString).toList.addString(new StringBuilder(), " | ")).toString + "\n"
  }
  
  override def toString: String = {
    "Name: " + name + "\n" + 
    "Fields: " + "\n" + (fieldList.map(field => field.toString).toList.addString(new StringBuilder(), " | ")).toString + "\n" +
    rows.map(row => row.toString).toList.addString(new StringBuilder(), "\n") + "\n"
  }
  
  def insert(data: Row): Table =
    new Table(name, fieldList, (rows :+ data))

  def delete(exp: BooleanExpression): Table = exp match {
    case Equal(fieldName: String, compareValue: DataType) => new Table(name, fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) != compareValue)))
    case NotEqual(fieldName: String, compareValue: DataType) => new Table(name, fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) == compareValue)))
    case LessThan(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type"); this }
      case other => new Table(name, fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) <= compareValue)))
    }
    case GreaterThan(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparision not operable on type"); this }
      case other => new Table(name, fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) >= compareValue)))
    }
    case LTE(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type"); this }
      case other => new Table(name, fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) < compareValue)))
    }
    case GTE(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case bool: BoolType => { println("Comparison not operable on type"); this }
      case other => new Table(name, fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) > compareValue)))
    }
    case NullExp() => new Table(name, fieldList, Nil)
  }

  def update(updateFieldName: String, newValue: DataType, exp: BooleanExpression): Table = exp match {
    case Equal(fieldName: String, compareValue: DataType) => new Table(name, fieldList, (ohHAI(rows)(row => row.getData(this.getField(fieldName)) == compareValue)(row => row.setData(this.getField(updateFieldName), newValue))))
    case NotEqual(fieldName: String, compareValue: DataType) => new Table(name, fieldList, (ohHAI(rows)(row => row.getData(this.getField(fieldName)) != compareValue)(row => row.setData(this.getField(updateFieldName), newValue))))
    case LessThan(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type boolean"); this }
      case other => new Table(name, fieldList, (ohHAI(rows)(row => compareValue < row.getData(this.getField(fieldName)))(row => row.setData(this.getField(updateFieldName), newValue))))
    }
    case GreaterThan(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type boolean"); this }
      case other => new Table(name, fieldList, (ohHAI(rows)(row => compareValue > row.getData(this.getField(fieldName)))(row => row.setData(this.getField(updateFieldName), newValue))))
    }
    case LTE(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type boolean"); this }
      case other => new Table(name, fieldList, (ohHAI(rows)(row => compareValue <= row.getData(this.getField(fieldName)))(row => row.setData(this.getField(updateFieldName), newValue))))
    }
    case GTE(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type boolean"); this }
      case other => new Table(name, fieldList, (ohHAI(rows)(row => compareValue >= row.getData(this.getField(fieldName)))(row => row.setData(this.getField(updateFieldName), newValue))))
    }
    case NullExp() => new Table(name, fieldList, (rows.map(row => row.setData(this.getField(updateFieldName), newValue))))
  }
  
  def toView(exp: BooleanExpression): View = exp match {
    case Equal(fieldName: String, compareValue: DataType) => new View(fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) == compareValue)))
    case NotEqual(fieldName: String, compareValue: DataType) => new View(fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) != compareValue)))
    case LessThan(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type"); new View(null, null) }
      case other => new View(fieldList, (rows.filterNot(row => row.getData(this.getField(fieldName)) <= compareValue)))
    }
    case GreaterThan(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparision not operable on type"); new View(null, null) }
      case other => new View(fieldList, (rows.filterNot(row => row.getData(this.getField(fieldName)) >= compareValue)))
    }
    case LTE(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type"); new View(null, null) }
      case other => new View(fieldList, (rows.filterNot(row => row.getData(this.getField(fieldName)) < compareValue)))
    }
    case GTE(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case bool: BoolType => { println("Comparison not operable on type"); new View(null, null) }
      case other => new View(fieldList, (rows.filterNot(row => row.getData(this.getField(fieldName)) > compareValue)))
    }
    case NullExp() => new View(fieldList, rows)
  }
}

/*
 * VIEW CLASS
 */

class View(fieldList: List[Field], rows: List[Row]) {
  
  def getFields: List[Field] =
    fieldList

  def getField(fieldName: String): Field =
    fieldList.find(elem => elem.getFieldName == fieldName).get
    
  def getRows: List[Row] =
    rows

  def printView: Unit = {
    println("Fields: ")
    println(fieldList.foreach(field => print(field.toString + " | ")))
    rows.foreach(row => println(row.printRow))
  }
  
  override def toString: String = {
    "Fields: " + "\n" + fieldList.map(field => field.toString).toList.addString(new StringBuilder(), " | ").toString + "\n" +
    rows.map(row => row.toString).toList.addString(new StringBuilder(), "\n").toString + "\n" 
  }
  
  def selectFromView(exp: BooleanExpression): View = exp match {
    case Equal(fieldName: String, compareValue: DataType) => new View(fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) == compareValue)))
    case NotEqual(fieldName: String, compareValue: DataType) => new View(fieldList, (rows.filter(row => row.getData(this.getField(fieldName)) != compareValue)))
    case LessThan(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type"); new View(null, null) }
      case other => new View(fieldList, (rows.filterNot(row => row.getData(this.getField(fieldName)) <= compareValue)))
    }
    case GreaterThan(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparision not operable on type"); new View(null, null) }
      case other => new View(fieldList, (rows.filterNot(row => row.getData(this.getField(fieldName)) >= compareValue)))
    }
    case LTE(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case x: BoolType => { println("Comparison not operable on type"); new View(null, null) }
      case other => new View(fieldList, (rows.filterNot(row => row.getData(this.getField(fieldName)) < compareValue)))
    }
    case GTE(fieldName: String, compareValue: DataType) => this.getField(fieldName) match {
      case bool: BoolType => { println("Comparison not operable on type"); new View(null, null) }
      case other => new View(fieldList, (rows.filterNot(row => row.getData(this.getField(fieldName)) > compareValue)))
    }
    case NullExp() => new View(fieldList, rows)
  }
  
  def asProjectedView(flist: List[String]): View = {
    new View(flist.map(name => this.getField(name)), rows.map(row => new Row(row.getData.filter(pair => flist.contains(pair._1.getFieldName)))))
  }
  
  def orderBy(fname: String): View = {
    new View(fieldList, rows.sortWith(_.getData(this.getField(fname)) > _.getData(this.getField(fname))))
  }
  
  def join(that: View): View = {
    var newRows = List[Row]()
    for(r <- rows) {
      for(r2 <- that.getRows){
        newRows = newRows :+ new Row(r.getData ++ r2.getData)
      }
    }
    new View((fieldList ++ that.getFields), newRows)
  }
  
  def intersect(that: View): View = 
    new View(fieldList, rows.intersect(that.getRows))
  
  def union(that: View): View = 
    new View(fieldList, (rows ++ that.getRows).distinct)
  
  def minus(that: View): View =
    new View(fieldList, rows.filterNot(row => that.getRows.contains(row)))
}

/*
 * ROW CLASS
 */

class Row(data: List[(Field, DataType)]) {

  def printRow: Unit =
    data.foreach(elem => print(elem._2.toString + "\t"))

  def getData(field: Field): DataType =
    data.find(elem => elem._1 == field).get._2

  def setData(field: Field, value: DataType): Row =
    new Row(data.updated(data.indexOf((field, this.getData(field))), (field, value)))
  
  def getData: List[(Field, DataType)] =
    data
    
  override def toString: String =
    data.map(elem => elem._2.toString).toList.addString(new StringBuilder(), "\t").toString

}

/*
 * FIELD CLASS
 */

abstract class Field(name: String) {
  override def toString: String
  def getFieldName: String =
    name
}
case class NumberField(name: String) extends Field(name) {
  override def toString: String =
    name + ": Integer"
}
case class DateField(name: String) extends Field(name) {
  override def toString: String =
    name + ": Date"
}
case class RealField(name: String) extends Field(name) {
  override def toString: String =
    name + ": Real"
}
case class VarcharField(name: String) extends Field(name) {
  override def toString: String =
    name + ": Varchar"
}
case class BoolField(name: String) extends Field(name) {
  override def toString: String =
    name + ": Boolean"
}
  

  
