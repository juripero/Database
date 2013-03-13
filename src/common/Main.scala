package common
import grammar._
import scala.util.parsing.combinator._

object Main extends App {
  
  /*println("**********************" +
  		  "* Scala Database App *" +
  		  "*                    *" +
  		  "*      Coded By      *" +
  		  "*    Tyler Sullens   *" +
  		  "*      Fall 2012     *" +
  		  "**********************")
   */
  
  while(true){
    print(">")
    var input = readLine()
    while(!input.contains(";")){
      print(">")
      input = (input ++ " ") ++ readLine()
    }
    DatabaseGrammar.parse(input)
  }

}