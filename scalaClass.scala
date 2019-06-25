package retail

class OrderItem
(
  var orderItemId: Int,
  var orderItemOrderId: Int,
  var orderItemProductId: Int,
  var orderItemQuantity: Int,
  var orderItemSubtotal: Float,
  var orderItemProductPrice: Float
) {
  //require is a custom function definition where in we dont have to add "def" in front of it. It checks the condition and if the condition does not match then returns the error
 require(orderItemSubtotal == orderItemQuantity * orderItemProductPrice,"Invalid orderItemSubtotal")
  //constructor
  def this(
  orderItemId: Int,
  orderItemOrderId: Int,
  orderItemProductId: Int,
  orderItemQuantity: Int,
  orderItemProductPrice: Float
  ) = {this(orderItemId, orderItemOrderId, orderItemProductId, orderItemQuantity, orderItemQuantity * orderItemProductPrice, orderItemProductPrice)}
//override toString
  override def toString = "OrderItem("+orderItemId+","+orderItemOrderId+","+orderItemProductId+","+orderItemQuantity+","+orderItemSubtotal+","+orderItemProductPrice+")"
}

//Right click --> Run Scala Console --> In scala console, run,  "val a = new OrderItem(1,1,1,2,80)"
