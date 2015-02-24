package edu.ucsc

import java.io.{FileWriter, File, FileReader}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.saddle.{Vec, Index, Series}
import scala.io.Source.fromFile


object DataFrame {

  def load_csv(sc: SparkContext, path: String, separator : Char =  ',', minPartitions : Int=2) : (RDD[(String,Series[String,Double])], Index[String]) = {
    val file = fromFile(path)
    val header_line = file.getLines().next()
    val header = parseLine(header_line, separator).drop(1)
    val header_br = sc.broadcast(header)

    val start_rdd = sc.textFile(path, minPartitions).mapPartitionsWithIndex( (part, lines) => {
      val z = lines.map( x => {
        parseLine(x,separator)
      })
      if (part == 0)
        z.drop(1)
      else
        z
    }, true).map( x => {
      (x(0), x.slice(1, x.length))
    })

    val convert_rdd = start_rdd.map( x => {
      (x._1, x._2.map( y => {
        y match {
          case "" =>
            Double.NaN
          case "NA" =>
            Double.NaN
          case _ =>
            y.toDouble
        }
      }
      ))
    } )

    val series_rdd = convert_rdd.map( x => {
      (x._1, Series(Vec(x._2), Index(header_br.value)))
    })
    return (series_rdd, Index(header))
  }

  def parseLine(line:String, separator: Char = ',') : Array[String] = {
    line.split(separator)
  }

}

