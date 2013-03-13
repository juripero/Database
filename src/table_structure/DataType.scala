package table_structure
import java.text.SimpleDateFormat
import java.util.Date

abstract class DataType(val value: Any) extends Ops {
  override def toString: String
}

case class BoolType(override val value: Boolean) extends DataType {
  override def toString: String = value.toString
}
case class NumberType(override val value: Int) extends DataType {
  override def toString: String = value.toString
}
case class DateType(override val value: Date) extends DataType {
  override def toString: String = (new SimpleDateFormat("MM/dd/yyyy")).format(this.value)
}
case class VarcharType(override val value: String) extends DataType {
  override def toString: String = value
}
case class RealType(override val value: Double) extends DataType {
  override def toString: String = value.toString
}

trait Ops {
  
  def <=(that: DataType): Boolean = this match {
    case num: NumberType =>  that.asInstanceOf[NumberType].value < num.value || that.asInstanceOf[NumberType].value == num.value
    case date: DateType => that.asInstanceOf[DateType].value.compareTo(date.value) match { case -1 => true case 0 => false case 1 => false}
    case varchar: VarcharType => that.asInstanceOf[VarcharType].value < varchar.value || that.asInstanceOf[VarcharType].value == varchar.value
    case real: RealType => that.asInstanceOf[RealType].value < real.value || that.asInstanceOf[RealType].value == real.value
  }
  
  def <(that: DataType): Boolean = this match {
    case num: NumberType =>  that.asInstanceOf[NumberType].value < num.value
    case date: DateType => that.asInstanceOf[DateType].value.compareTo(date.value) match { case -1 => true case 0 => true case 1 => false}
    case varchar: VarcharType => that.asInstanceOf[VarcharType].value < varchar.value
    case real: RealType => that.asInstanceOf[RealType].value < real.value
  }
  
  def >=(that: DataType): Boolean = this match {
    case num: NumberType => that.asInstanceOf[NumberType].value > num.value || that.asInstanceOf[NumberType].value == num.value
    case date: DateType => that.asInstanceOf[DateType].value.compareTo(date.value) match { case -1 => false case 0 => false case 1 => true}
    case varchar: VarcharType => that.value.asInstanceOf[String] > varchar.value || that.asInstanceOf[VarcharType].value == varchar.value
    case real: RealType => that.value.asInstanceOf[Double] > real.value || that.asInstanceOf[RealType].value == real.value
  }
  
  def >(that: DataType): Boolean = this match {
    case num: NumberType => that.asInstanceOf[NumberType].value > num.value
    case date: DateType => that.asInstanceOf[DateType].value.compareTo(date.value) match { case -1 => false case 0 => true case 1 => true}
    case varchar: VarcharType => that.asInstanceOf[VarcharType].value > varchar.value
    case real: RealType => that.asInstanceOf[RealType].value > real.value
  }
}