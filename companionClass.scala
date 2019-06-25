package retail


//Here class A and object A are companion objects or companion classes as they both share the same name.
//Companion object can access private variables of companion classes and vice versa.
class A (private val message: String)

object A{
  val a = new A("Yo Yo Honey Singh..!!")
  println(a.message)
}

//Right click --> Run Scala Console --> In scala console, run,  "A"
//Alt+Enter for imports
//Ctrl+Enter for running commands

/**
object B{
  val a = new A("Yo Yo Honey Singh..!!")
  println(a.message)
}

//In the object B, a.message will show error --> "Symbol message is inaccessible from this place"
This is because class A and object B are not companion classes/objects. And private variables of a class cannot be accessed from another class/object.
**/
