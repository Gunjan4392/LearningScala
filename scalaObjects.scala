//Scala does not support keyword static
//Objects are basically singleton classes or static classes
object sampleObject {
def sum(a:Int, b:Int): Int =
{  a+b  }

def main(args: Array[String])
{
  val a = args(0).toInt
  val b = args(1).toInt
val c = sum(a,b)
  println("sum is " + c)
}
}
