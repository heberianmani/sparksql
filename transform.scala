package org.inceptez.spark.sql.reusablefw
case class customercaseclass(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)
class transform  extends java.io.Serializable
{
   def generatemailid(i:String,j:String):String=
    { // Mohamed irfan ,001
    return i.trim().capitalize.replace(" ",".").replace("-",".")+j+"@inceptez.in"
    }
   
    def getcategory(i:String):String=i.trim() match
    {
      case "Police officer"|"Politician"|"Judge"=> "Government"
      case "Automotive mechanic"|"Economist"|"Loan officer"=> "Worker"
      case "Psychologist"|"Veterinarian"|"Doctor"=> "Medical"
      case "Civil engineer"|"Computer hardware engineer"|"Engineering technician"=> "Engineering"  
      case _ => "Others"
    }
}