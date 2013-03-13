package common
import table_structure._

class TableCollection(data: Map[String, Table]) {
  
  def addTable(tab: Table): TableCollection = 
    new TableCollection(data + (tab.getTableName -> tab))
  
  def removeTable(tab: String): TableCollection = 
    new TableCollection(data - tab)
  
  def printCollection: Unit = 
    data.values.foreach(tab => tab.printTabInfo)
  
  def socketPrintCollection: String = {
    data.values.map(tab => tab.stringTabInfo).toList.addString(new StringBuilder(), "\n").toString
  }
  
  def getTable(name: String): Table =
    data.get(name).get
    
  def containsTable(name: String): Boolean =
    data.contains(name)
      
  def updateTable(name: String, tab: Table): TableCollection = 
    new TableCollection(data.updated(name, tab))
}


  