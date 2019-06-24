//Function Call


package com.impetus.spark.sparksql

object itversity {
  
//Normal Function -- Not mandatory to have a return type specified
  def fact (i: Int) =
  {
    var res = 1
    for ( e <-i to 1 by -1)
      res *= e
      res
  }
  
  //Recursive Function -- Recursive Function must have a return type specified
  def factr(i: Int): Int =
  {
    if(i == 1) 1 else i * fact(i-1) 
  }
  
  //Main function
    def main(args: Array[String]) {  
      println("Factorial of 5 = " + fact(5))
      println("Factorial Recursive of 5 = " + factr(5))
    }
}

