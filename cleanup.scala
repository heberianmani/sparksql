package org.inceptez.spark.sql.reusablefw

class cleanup extends java.io.Serializable {
  def trimupperdsl(i:String):String=
    { // 100 lines of business logic
    return i.trim().toUpperCase()}
  
  def concatnames(i:String,j:String):String=
    {return j+" "+i.trim().toUpperCase()}

  def trimupper(i:String):String=
    {return i.trim().toUpperCase()}

}