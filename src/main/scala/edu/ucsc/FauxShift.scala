package edu.ucsc

import org.saddle.{Vec, Series, Index}
import org.saddle.io._

import org.apache.spark.graphx.{Edge, Graph}

import org.apache.spark.SparkContext._
import scala.collection.JavaConverters._
import org.rogach.scallop
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class ShiftVertex(val name : String,
                  val nodeType : String,
                  val input: Series[String,Double],
                  val knockoutTests: Index[String]
                   ) extends Serializable{

  var levels: Series[String,Double] = null

  def process(messages: List[ShiftMessage]) : ShiftVertex = {
    val out = new ShiftVertex(name, nodeType, input, knockoutTests)
    //calculate levels here, this involves taking all of the incoming messages, and the original inputs and determining
    //the current levels
    out.levels = null
    return out
  }

}

class ShiftMessage(val edgeType: String, val knockOut:String, val knockOutDir: String, val values: Series[String,Double]) extends Serializable {

}

object FauxShift {

  def main(args: Array[String]) = {

    object cmdline extends scallop.ScallopConf(args) {
      val spark: scallop.ScallopOption[String] = opt[String]("spark", default = Option("local"))
      val spf: scallop.ScallopOption[String] = opt[String]("spf")
      val mutations: scallop.ScallopOption[String] = opt[String]("mut")
      val expression: scallop.ScallopOption[String] = opt[String]("exp")
      val cycle_count: scallop.ScallopOption[Int] = opt[Int]("cycles")
    }

    val sconf = new SparkConf().setMaster(cmdline.spark()).setAppName("ShiftTest")
    sconf.set("spark.serializer.objectStreamReset", "1000")
    val sc = new SparkContext(sconf)
    val (vertices, edges) = SPFReader.read(cmdline.spf())
    val vertex_rdd = sc.parallelize(vertices)
    val edge_rdd = sc.parallelize(edges.map(x => Edge(x._1, x._2, x._3)))
    val graph = Graph(vertex_rdd, edge_rdd)

    //open the TSV
    val exp_file = CsvFile(cmdline.expression())
    //Parse it (with row index and column index)
    val exp_frame = CsvParser.parse(params=CsvParams(separChar='\t'))(exp_file).withRowIndex(0).withColIndex(0).mapValues(CsvParser.parseDouble)

    val experimentSet = exp_frame.rowIx
    val experimentSet_br = sc.broadcast(experimentSet)

    //parallelize the columns
    val exp_rdd = sc.parallelize(exp_frame.toColSeq)
    //join the columns to the graph vertices by the vertex names
    val exp_rdd_joined = vertex_rdd.map(x => (x._2.name, x)).join(exp_rdd).map(
      //create new vertex values, which are ShiftVertex structures
      x => (x._2._1._1, new ShiftVertex(x._2._1._2.name, x._2._1._2.nodeType, x._2._2, experimentSet_br.value) )
    )

    var data_graph = graph.outerJoinVertices(exp_rdd_joined)(
      //joint the generated ShiftVertex structures to the graph, if a node isn't matched give it
      //an empty set of input values
      (x,y,z) => { z.getOrElse(new ShiftVertex(y.name, y.nodeType, Series[String, Double](), experimentSet_br.value)) }
    )

    for (i <- 1 to cmdline.cycle_count()) {
      val messages = data_graph.aggregateMessages[List[ShiftMessage]](x => {
        //The edge passing rules go here
        //do a cycle over the different 'experiments'. Each experiment is different
        //gene to be knocked out
        x.srcAttr.knockoutTests.toSeq.foreach( knockout => {
          //the upstream edge removal messages
          if (x.dstAttr.name != knockout) {
            x.sendToDst(List(new ShiftMessage(x.attr.edgeType, knockout, "up", x.srcAttr.levels)))
          }
          //the downstream edge removal messages
          if (x.srcAttr.name != knockout) {
            x.sendToDst(List(new ShiftMessage(x.attr.edgeType, knockout, "down", x.srcAttr.levels)))
          }
        }
        )
      },
        (x, y) => {
          val out = x ++ y
          out
        }
      )
      val next = data_graph.outerJoinVertices(messages)((vid, vertex, messages) => vertex.process(messages.getOrElse(List[ShiftMessage]()) ))
      data_graph = next
    }

    println(data_graph.vertices.collect.mkString(","))
  }
}

