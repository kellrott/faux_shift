package edu.ucsc

import org.saddle.{Vec, Series}
import org.saddle.io._

import org.apache.spark.graphx.{TripletFields, EdgeDirection, Edge, Graph}

import org.apache.spark.SparkContext._
import scala.collection.JavaConverters._
import org.rogach.scallop
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class ShiftVertex(val name : String, val nodeType : String, val level: Double) extends Serializable{

}

object FauxShift {

  def main(args: Array[String]) = {

    object cmdline extends scallop.ScallopConf(args) {
      val spark: scallop.ScallopOption[String] = opt[String]("spark", default = Option("local"))
      val spf: scallop.ScallopOption[String] = opt[String]("spf")
      val mutations: scallop.ScallopOption[String] = opt[String]("mut")
      val expression: scallop.ScallopOption[String] = opt[String]("exp")
    }

    val sconf = new SparkConf().setMaster(cmdline.spark()).setAppName("ShiftTest")
    //sconf.set("spark.local.dir", conf.localdisk())
    sconf.set("spark.serializer.objectStreamReset", "1000")
    //sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sconf.set("spark.kryo.registrator", "sparkgremlin.common.SparkGremlinKyroRegistrator")
    val sc = new SparkContext(sconf)
    val (vertices, edges) = SPFReader.read(cmdline.spf())
    val vertex_rdd = sc.parallelize(vertices)
    val edge_rdd = sc.parallelize(edges.map(x => Edge(x._1, x._2, x._3)))
    val graph = Graph(vertex_rdd, edge_rdd)

    //open the TSV
    val exp_file = CsvFile(cmdline.expression())
    //Parse it (with row index and column index)
    val exp_frame = CsvParser.parse(params=CsvParams(separChar='\t'))(exp_file).withRowIndex(0).withColIndex(0).mapValues(CsvParser.parseDouble)

    //for every label, obtain the mean across observations
    val default_value = exp_frame.reduce( x => x.sum / x.count )
    //in cases where a label is missing, replace with the mean
    //exp_frame = exp_frame.rmapVec( x => x.zipMap(default_value.toVec)((y,z) => if (y.isNaN) z else y))

    //parallelize the columns
    val exp_rdd = sc.parallelize(exp_frame.toColSeq)
    //join the columns to the graph vertices
    val exp_rdd_joined = vertex_rdd.map(x => (x._2.name, x._1)).join(exp_rdd).map(x => (x._2._1, x._2._2) )

    val default_values = exp_frame.rreduce( x => x.sum / x.count  )
    val default_values_br = sc.broadcast(default_values)
    //data_graph should have a Series[String,Double] attached to it, with the
    //sample -> expressions pairs (or empty if there was no data)
    val data_graph = graph.outerJoinVertices(exp_rdd_joined)(
      (x,y,z) => { (y,z.getOrElse(default_values_br.value)) }
    )

    val step = data_graph.aggregateMessages[ (Int, Series[String,Double]) ]( x => {
        x.sendToDst( (1,x.srcAttr._2) )
        x.sendToSrc( (1,x.dstAttr._2) )
      },
      (x,y) => {
        val out = x._2 + y._2
        (x._1 + y._1, out)
      }
    ).mapValues( x => x._2 / x._1)

    val out = data_graph.outerJoinVertices(step)( (vid, old, newOpt) => (old._1, newOpt.getOrElse(Series[String,Double]())))
    println(out.vertices.collect.mkString(","))

  }
}

