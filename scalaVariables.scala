/**	Variables
		val -- immutable -- cant change the value of variable
		var -- mutable -- value of variable can be changed
		Initialization of variable is necessary in Scala
		Reassignment of variable is not allowed in case of val.
**/

//Declare Empty Variables

var longVar = 0:Long
var intVar = 0:Int
var arrayString = Array[String]()

//Print the greater number
		val c=(math.random * 100).toInt
		println("Value of c = " + c)
		val d=(math.random * 100).toInt
		println("Value of d = " + d)
		if (c>d)
			println(c + " is greater than " + d)
		else if (c==d)
			println("Both are equal")
		else if (c<d)
			println(c + " is smaller than " + d)

//Factorial of a number
	var res =1
	val a = 10
	for (e <- a to 2 by -1)
		res = res * e
		println("Factorial of "+ a + " is "+ res)

//Fibonacci Series of a number
	val a = 10
	var pre = 0
	var cur = 1
	println(pre)
	println(cur)
	var res = 0
	for (e <- 2 to a -1)
	{
		res = pre + cur
		println (res)
		pre =cur
		cur =res
	}
