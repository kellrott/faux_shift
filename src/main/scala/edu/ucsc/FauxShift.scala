package edu.ucsc

import org.saddle.scalar.NA
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
                  val exp_input: Series[String,Double]
                   ) extends Serializable{

  var levels: Series[(String,String,Boolean),Double] = null

  def init(init_val: Double, knockouts : Index[(String, String,Boolean)]) : ShiftVertex = {
    val v = knockouts.map( x => {
      if (!exp_input.contains(x._1) || exp_input.get(x._1).isNA) {
        init_val
      } else {
        exp_input.get(x._1).get
      }
    })
    val out = new ShiftVertex(name, nodeType, exp_input)
    out.levels = Series(v.toVec, knockouts)
    out
  }

  def process(messages: List[ShiftMessage]) : ShiftVertex = {
    val knockouts = messages.foldRight(Index[(String,String,Boolean)]())( (x,y) => x.values.index.union(y).index )
    println(messages)
    val results = knockouts.map( exp => {
      // -a> or -a| edge types
      val activation_value = Option(Vec( messages.filter( x => x.edgeType == "-a>" || x.edgeType == "-a|" ).
        map( x => x.values.get(exp) ).filter(!_.isNA).map(_.get): _* ).mean)

      // -t> or -t| edge types
      val transcription_value = Option(Vec( messages.filter( x => x.edgeType == "-t>" || x.edgeType == "-t|" ).
        map( x => x.values.get(exp) ).filter(!_.isNA).map(_.get): _*).mean)

      // -component> edge types
      val component_value = Vec( messages.filter( x => x.edgeType == "-component>" ).
        map( x => x.values.get(exp) ).filter(!_.isNA).map(_.get): _*).max

      // -member> edge types
      val member_value = Vec( messages.filter( x => x.edgeType == "-member>" ).
        map( x => x.values.get(exp) ).filter(!_.isNA).map(_.get):_* ).min

      val input_value = if (exp_input.contains(exp._1))
        Option(exp_input.get(exp._1).get)
      else
        None

      val inputs = Array(Option(1.0), activation_value, transcription_value, component_value, member_value, input_value)
      //println(name, inputs.mkString(" , "))
      Vec(inputs.filter(x => x.isDefined).map(_.get)).mean
    }).toVec

    val out = new ShiftVertex(name, nodeType, exp_input)
    //calculate levels here, this involves taking all of the incoming messages, and the original inputs and determining
    //the current levels
    out.levels = Series(results, knockouts)
    return out
  }

}

class ShiftMessage(val edgeType: String, val downStream: Boolean, val values: Series[(String,String,Boolean),Double]) extends Serializable {

  override def toString() : String = {
    "ShiftMessage<%s:%s=%s>".format(edgeType,downStream,values)
  }
}

object FauxShift {

  val DOWNSTREAM = true
  val UPSTREAM = false

  def main(args: Array[String]) = {

    object cmdline extends scallop.ScallopConf(args) {
      val spark: scallop.ScallopOption[String] = opt[String]("spark", default = Option("local"))
      val spf: scallop.ScallopOption[String] = opt[String]("spf")
      val mutations: scallop.ScallopOption[String] = opt[String]("mut")
      val expression: scallop.ScallopOption[String] = opt[String]("exp")
      val cycle_count: scallop.ScallopOption[Int] = opt[Int]("cycles", default=Option(10))
      val sample: scallop.ScallopOption[String] = opt[String]("sample")
      val gene: scallop.ScallopOption[String] = opt[String]("gene")
    }

    val sconf = new SparkConf().setMaster(cmdline.spark()).setAppName("ShiftTest")
    sconf.set("spark.serializer.objectStreamReset", "1000")
    val sc = new SparkContext(sconf)
    val (vertices, edges) = SPFReader.read(cmdline.spf())
    val vertex_rdd = sc.parallelize(vertices, 20)
    val edge_rdd = sc.parallelize(edges.map(x => Edge(x._1, x._2, x._3)), 20)
    val graph = Graph(vertex_rdd, edge_rdd)

    //open the TSV
    val exp_file = CsvFile(cmdline.expression())
    //Parse it (with row index and column index)
    var exp_frame = CsvParser.parse(params=CsvParams(separChar='\t'))(exp_file).withRowIndex(0).withColIndex(0).mapValues(CsvParser.parseDouble).T
    //columns are samples
    //rows are genes

    if (cmdline.sample.isDefined) {
      //if we are only looking at a single sample, we can remove all other columns
      exp_frame = exp_frame.colSliceBy(cmdline.sample(),cmdline.sample())
    }

    //true is downstream, false is upstream
    //println(exp_frame.rowIx)
    val experimentSet = if (cmdline.gene.isDefined) {
      //for every sample, define a gene:UPSTREAM and a gene:DOWNSTREAM  experiment
      Index(exp_frame.toColSeq.flatMap(x => List((x._1, cmdline.gene(), UPSTREAM), (x._1, cmdline.gene(), DOWNSTREAM))).toArray)
    } else {
      Index(exp_frame.toColSeq.flatMap(x => x._2.toSeq.map(y => (x._1, y._1, UPSTREAM)) ++ x._2.toSeq.map(y => (x._1, y._1, DOWNSTREAM))).toArray)
    }
    if (!experimentSet.isUnique) {
      //make sure that duplicate experiments haven't been defined, this would cause problems later in the message intersection code
      println(experimentSet)
      throw new Exception("Duplicate item in index")
    }
    val experimentSet_br = sc.broadcast(experimentSet)

    //parallelize the matrix by the genes (rows) so the data can be attached to the graph
    val exp_rdd = sc.parallelize(exp_frame.toRowSeq, 20)
    //join the columns to the graph vertices by the vertex names
    val exp_rdd_joined = vertex_rdd.map(x => (x._2.name, x)).join(exp_rdd).map(
      //create new vertex values, which are ShiftVertex structures
      x => (x._2._1._1, new ShiftVertex(x._2._1._2.name, x._2._1._2.nodeType, x._2._2) )
    )
    //join the newly created ShiftVertexs with the existing graph structure
    var data_graph = graph.outerJoinVertices(exp_rdd_joined)(
      //joint the generated ShiftVertex structures to the graph, if a node isn't matched give it
      //an empty set of input values
      (x,y,z) => { z.getOrElse(new ShiftVertex(y.name, y.nodeType, Series())) }
    )

    //data_graph.vertices.map( x=> x._2.name + "\t" + x._2.exp_input).saveAsTextFile("exp.dump")
    data_graph = data_graph.mapVertices( (x,y) => y.init(0.5, experimentSet_br.value) )
    //data_graph.vertices.map( x=> x._2.name + "\t" + x._2.levels.toSeq.map( y => "%s:%s=%s".format(y._1._1, y._1._2, y._2) ).mkString("\t") ).saveAsTextFile("cycle." + 0 + ".dump")
    data_graph.vertices.map( x=> x._2.name + "\t" + x._2.levels.toSeq.map( y => "%s:%s:%s=%s".format(y._1._1, y._1._2, y._1._3, y._2) ).mkString("\t") ).saveAsTextFile("start.dump")

    for (i <- 1 to cmdline.cycle_count()) {
      val messages = data_graph.aggregateMessages[List[ShiftMessage]](x => {
        //The edge passing rules go here
        //do a cycle over the different 'experiments'. Each experiment is different
        //gene to be knocked out

        if (x.srcAttr.levels != null ) {
          //downstream messages
          val downstream = x.srcAttr.levels.filterIx(y => {
            //either the downstream target isn't part of the knockout experiment, or it isn't a downstream experiment
            y._1 != x.dstAttr.name || y._2 == UPSTREAM
          })
          x.sendToDst(List(new ShiftMessage(x.attr.edgeType, DOWNSTREAM, downstream)))
        }
        if (x.dstAttr.levels != null) {
          //upstream messages
          val upstream = x.dstAttr.levels.filterIx(y => {
            //either the downstream target isn't part of the knockout experiment, or it isn't a downstream experiment
            y._1 != x.srcAttr.name || y._2 == DOWNSTREAM
          })
          x.sendToSrc(List(new ShiftMessage(x.attr.edgeType, UPSTREAM, upstream)))
        }
      },
        (x, y) => {
          val out = x ++ y
          out
        }
      )
      val next = data_graph.outerJoinVertices(messages)((vid, vertex, messages) => vertex.process(messages.getOrElse(List[ShiftMessage]()) ))
      data_graph = next
      data_graph.vertices.map( x=> x._2.name + "\t" + x._2.levels.toSeq.map( y => "%s:%s:%s=%s".format(y._1._1, y._1._2, y._1._3, y._2) ).mkString("\t") ).saveAsTextFile("cycle." + i + ".dump")
    }

    //val out_data = data_graph.vertices.map( x=> (x._2.name, x._2.levels) )
    //println(out_data.collect.mkString("\n"))

  }

}

