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
    val userDefinedProperties = args(0)
    userDefinedProperties.stripMargin.split(",").foreach(
      str => {
        val (name, value) = str.stripMargin.split("=")
        property.setProperty(name, value)
      }
    )
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
    get(data, default.toString).split(",").map(v => v.toDouble)
  }
}
