package util

import java.util.Properties
import java.io.InputStream
import java.io.FileInputStream

class ConfigLoader(args: Array[String], configFile: String) extends Serializable {
  val property = new Properties()
  def load(): ConfigLoader = {
    var input: InputStream = null
    input = new FileInputStream(configFile)
    property.load(input)
    this
  }

  def update(): ConfigLoader ={
    if(args.length != 0){
      val userDefinedProperties: String = args(0)
      if(!userDefinedProperties.isEmpty){
        userDefinedProperties.stripMargin.split(",").foreach(
          str => {
            val name_value = str.stripMargin.split("=")
            property.setProperty(name_value(0), name_value(1))
          }
        )
      }
    }
    this
  }

  def get(data: String, default: String): String ={
    property.getProperty(data, default)
  }

  def getBoolean(data: String, default: Boolean): Boolean ={
    get(data, default.toString).toBoolean
  }

  def getInt(data: String, default: Int): Int = {
    get(data, default.toString).toInt
  }

  def getDouble(data: String, default: Double): Double = {
    get(data, default.toString).toDouble
  }

  def getDoubleArray(data: String, default: Array[Double]): Array[Double] = {
    get(data, default.toString).split("-").map(v => v.toDouble)
  }
}
