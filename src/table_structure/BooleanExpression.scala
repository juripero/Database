package table_structure
import table_structure._

abstract class BooleanExpression
case class Equal(fieldName: String, value: DataType) extends BooleanExpression
case class NotEqual(fieldName: String, value: DataType) extends BooleanExpression
case class LessThan(fieldName: String, value: DataType) extends BooleanExpression
case class GreaterThan(fieldName: String, value: DataType) extends BooleanExpression
case class LTE(fieldName: String, value: DataType) extends BooleanExpression
case class GTE(fieldName: String, value: DataType) extends BooleanExpression
case class NullExp() extends BooleanExpression
