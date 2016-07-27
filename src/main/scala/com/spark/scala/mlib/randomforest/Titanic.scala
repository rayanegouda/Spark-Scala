package com.spark.scala.mlib.randomforest
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ SparseVector, DenseVector, Vector, Vectors }
import scala.util.control.NonFatal
import scala.collection.immutable.{ Map, HashMap }
import org.apache.spark.rdd.RDD
import java.util.Properties

class Titanic (line: String) {

  var features: Map[String, String] = {
    val record = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)
    if (record.size == 12) {
      var Array(passengerId, survived, pclass, name, sex, age, sibSp, parch, ticket, fare, cabin, embarked) = record
      if(age.length()==0){
        age = "40" // should really be something like average of all ages
      }
      Map("passengerId" -> passengerId, "survived" -> survived, "pclass" -> pclass,
        "name" -> name.replace("\"", ""), "sex" -> sex, "age" -> age, "sibSp" -> sibSp, "parch" -> parch,
        "ticket" -> ticket, "fare" -> fare, "cabin" -> cabin, "embarked" -> embarked)
    } else if (record.size == 11) {
      
      var Array(passengerId, pclass, name, sex, age, sibSp, parch, ticket, fare, cabin, embarked) = record
      
      if(age.length()==0){
        age = "40"  //should really be something like average of all ages
      }
        Map("passengerId" -> passengerId, "pclass" -> pclass,
        "name" -> name.replace("\"", ""), "sex" -> sex, "age" -> age, "sibSp" -> sibSp, "parch" -> parch,
        "ticket" -> ticket, "fare" -> fare, "cabin" -> cabin, "embarked" -> embarked)
    } else {
      new HashMap() 
    }
  }

  def parseDouble(s: String, default: Double = 0): Double = try { s.toDouble } catch { case NonFatal(_) => default }
  def parseInt(s: String, default: Int = 0): Int = try { s.toInt } catch { case NonFatal(_) => default }
  def parseSex(s: String): Double = (if (s == "male") 1d else 0d)
  def toVector(): Vector = {
    return Vectors.dense(
      parseDouble(features("pclass")),
      parseSex(features("sex")),
      parseDouble(features("age")),
      parseDouble(features("sibSp")),
      parseDouble(features("parch")),
      parseDouble(features("ticket")),
      parseDouble(features("fare")) 
      //cabin
      //embarked
      )
  }
  
  def toLabeledPoint(): LabeledPoint = {
    // TODO check if the "survived" column is really there
    return LabeledPoint(parseDouble(features("survived")), toVector())
  }

  def loadData: Map[String, Any] = {
    val resultMap = Map("passengerId" -> this.features("passengerId").toInt, 
       "pclass" -> parseInt(this.features("pclass")),
       "name" -> this.features("name").replace("\"", ""), 
       "sex" -> this.features("sex"), 
       "age" -> parseDouble(this.features("age")), 
       "sibSp" -> parseInt(this.features("sibSp")), 
       "parch" -> parseInt(this.features("parch")),
       "ticket" -> this.features("ticket"), 
       "fare" -> parseDouble(this.features("fare")), 
       "cabin" -> this.features("cabin"), 
       "embarked" -> this.features("embarked"))
       if(this.features.contains("survived")) {
       resultMap updated ("survived", this.features("survived").toInt)  
       }
     resultMap
  }  
  
}