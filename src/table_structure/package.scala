package object table_structure {
  
  def ohHAI(lst: List[Row])(p: Row => Boolean)(f: Row => Row): List[Row] = {
    var xs = List[Row]()
    for(x <- lst) {
      if(p(x)) xs = xs :+ f(x)
      else xs = xs :+ x
    }
    xs
  }

}