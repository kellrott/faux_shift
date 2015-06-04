package edu.ucsc

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.saddle.{Vec, Series, Index}

import org.apache.spark.graphx.{VertexRDD, Edge, Graph}

import org.apache.spark.SparkContext._
import scala.collection.JavaConverters._
import org.rogach.scallop
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.reflect.io.File
import scala.io.Source.fromFile

class ShiftVertex(val name : String,
                  val nodeType : String,
                  val exp_input: Map[String,Double]
                   ) extends Serializable{

  var levels: Array[Double] = null

  def init(init_val: Double, knockouts : Array[(String, String,Boolean)]) : ShiftVertex = {
    val out = new ShiftVertex(name, nodeType, exp_input)
    out.levels = knockouts.map( x => {
      if (!exp_input.contains(x._1) || exp_input.get(x._1).get.isNaN) {
        init_val
      } else {
        exp_input.get(x._1).get
      }
    })
    out
  }

  def process(messages: List[ShiftMessage], knockouts: Array[(String,String,Boolean)], gravity:Double, inertia:Double) : ShiftVertex = {

    val results = knockouts.zipWithIndex.map( (exp) => {
      // -a> edge types
      val activation_value_pos = Option(Vec( messages.filter( x => x.edgeType == "-a>" ).
        map( x => x.values(exp._2) ).filter( x => !x.isNaN ).map(_.get): _* ).mean)

      // -a| edge types
      val activation_value_neg = Option(Vec( messages.filter( x => x.edgeType == "-a|" ).
        map( x => x.values(exp._2) ).filter( x => !x.get.isNaN ).map(1.0 - _.get): _* ).mean)

      // -t> edge types
      val transcription_value_pos = Option(Vec( messages.filter( x => x.edgeType == "-t>").
        map( x => x.values(exp._2) ).filter( x => !x.get.isNaN ).map(_.get): _*).mean)

      // -t| edge types
      val transcription_value_neg = Option(Vec( messages.filter( x => x.edgeType == "-t|" ).
        map( x => x.values(exp._2) ).filter( x => !x.get.isNaN ).map(1.0 - _.get): _*).mean)

      // -component> edge types
      val component_value = Vec( messages.filter( x => x.edgeType == "-component>" ).
        filter(_.downStream).
        map( x => x.values(exp._2) ).filter( x => !x.get.isNaN).map(_.get): _*).min

      // -member> edge types
      val member_value = Vec( messages.filter( x => x.edgeType == "-member>" ).
        filter(_.downStream).
        map( x => x.values(exp._2) ).filter( x => !x.get.isNaN).map(_.get):_* ).max

      val inputs = Array(activation_value_pos, activation_value_neg,
        transcription_value_pos, transcription_value_neg,
        component_value, member_value)
      //println(name, inputs.mkString(" , "))
      val input_val = Vec(inputs.filter(x => x.isDefined).map(_.get)).mean

      val new_value = if (exp_input.contains(exp._1._2) && !exp_input.get(exp._1._2).get.isNaN) {
        ( (gravity * exp_input.get(exp._1._2).get) + ((1.0 - gravity) * input_val))
      } else if (!input_val.isNaN) {
        input_val
      } else {
        0.5
      }
      val old_value = this.levels(exp._2)
      val out_value = (inertia * old_value) + ((1.0 - inertia) * new_value)
      out_value
    })

    val out = new ShiftVertex(name, nodeType, exp_input)
    out.levels = results
    return out
  }

  def toJSON(knockouts: Array[(String,String,Boolean)], fullOutput:Boolean=false): String = {
    val up_exp = new mutable.HashMap[String,mutable.HashMap[String,Double]]()
    val down_exp = new mutable.HashMap[String,mutable.HashMap[String,Double]]()

    knockouts.zipWithIndex.foreach( x => {
      if (x._1._3) {
        if (!down_exp.contains(x._1._1)) {
          down_exp(x._1._1) = new mutable.HashMap[String,Double]()
        }
        down_exp(x._1._1)(x._1._2) = levels(x._2)
      } else {
        if (!up_exp.contains(x._1._1)) {
          up_exp(x._1._1) = new mutable.HashMap[String,Double]()
        }
        up_exp(x._1._1)(x._1._2) = levels(x._2)
      }
    })

    val shift = new mutable.HashMap[String,Map[String,Double]]()
    up_exp.foreach( x => {
      shift(x._1) = x._2.keys.
        filter( y => (up_exp(x._1).contains(y)) && (down_exp(x._1).contains(y)) ).
        map( y => (y, down_exp(x._1)(y) - up_exp(x._1)(y) )).toMap
    } )

    val v = if (fullOutput) {
      new JSONObject(
        Map(
          "gene" -> name,
          "exp_inputs" -> JSONObject(exp_input.toSeq.toMap),
          "experiments" -> JSONObject(Map(
            "downstream" -> JSONObject(down_exp.map(x => (x._1, JSONObject(x._2.toMap))).toMap),
            "upstream" -> JSONObject(up_exp.map(x => (x._1, JSONObject(x._2.toMap))).toMap))
          ),
          "shift" -> JSONObject(shift.map(x => (x._1, JSONObject(x._2))).toMap)
        )
      )
    } else {
      new JSONObject(
        Map(
          "gene" -> name,
          "shift" -> JSONObject( shift.map(x => (x._1, JSONObject( Map(name -> x._2.getOrElse(name, Double.NaN))))).toMap )
        )
      )
    }
    v.toString()
  }

}

class ShiftMessage(
                    val edgeType: String,
                    val downStream: Boolean,
                    val values: Array[Double])
  extends Serializable {
  override def toString() : String = {
    "ShiftMessage<%s:%s=%s>".format(edgeType,downStream,values)
  }
}


class ShiftRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    //kryo.setRegistrationRequired(true)
    kryo.register(classOf[ShiftMessage])
    kryo.register(classOf[ShiftVertex])
    kryo.register(classOf[SPFVertex])
    kryo.register(classOf[SPFEdge])
    kryo.register(classOf[IndexedSeq[(String,Series[String,Double])]])
    kryo.register(classOf[org.saddle.index.IndexAny[_]])
    kryo.register(classOf[Series[String,Double]])
    kryo.register(classOf[String])
    kryo.register(classOf[scala.math.Ordering[_]])
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
      val cycle_count: scallop.ScallopOption[Int] = opt[Int]("cycles", default = Option(10))
      val intertia: scallop.ScallopOption[Double] = opt[Double]("inertia", default = Option(0.5))
      val gravity: scallop.ScallopOption[Double] = opt[Double]("gravity", default = Option(0.5))
      val frags: scallop.ScallopOption[Int] = opt[Int]("frags", default = Option(20))
      val gene: scallop.ScallopOption[String] = opt[String]("gene")
      val genelist: scallop.ScallopOption[String] = opt[String]("genelist")
      val outdir: scallop.ScallopOption[String] = opt[String]("outdir")
      val sample: scallop.ScallopOption[List[String]] = opt[List[String]]("sample")
      val setsize: scallop.ScallopOption[Int] = opt[Int]("setsize", default = Option(5))
      val checkpoint: scallop.ScallopOption[Int] = opt[Int]("checkpoint")
      val maxcores: scallop.ScallopOption[Int] = opt[Int]("maxcores")
    }

    val sconf = new SparkConf().setMaster(cmdline.spark()).setAppName("ShiftTest")
    sconf.set("spark.serializer.objectStreamReset", "100")
    sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sconf.set("spark.kryo.registrator", "edu.ucsc.ShiftRegistrator")
    sconf.set("spark.akka.frameSize", "50")
    //sconf.set("spark.logConf", "false")
    if (cmdline.maxcores.isDefined) {
      sconf.set("spark.cores.max", cmdline.maxcores().toString )
      sconf.set("spark.mesos.coarse", "true")
    }

    val sc = new SparkContext(sconf)
    val (vertices, edges) = SPFReader.read(cmdline.spf())
    val vertex_rdd = sc.parallelize(vertices, cmdline.frags())
    val edge_rdd = sc.parallelize(edges.map(x => Edge(x._1, x._2, x._3)), cmdline.frags())
    val graph = Graph(vertex_rdd, edge_rdd)

    val outdir = File(cmdline.outdir())
    if (!outdir.exists) outdir.createDirectory()

    var (input_frame, col_index) = DataFrame.load_csv(sc, cmdline.expression(), separator = '\t')

    if (cmdline.sample.isDefined) {
      //if we are only looking at a single sample, we can remove all other columns
      val cmd_sample = cmdline.sample()
      input_frame = input_frame.filter(x => cmd_sample.contains(x._1))
    }

    val samples = input_frame.map( _._1 ).collect()

    //move along the samples in groups of size 'setsize'
    samples.grouped(cmdline.setsize()).zipWithIndex.foreach( exp_set => {
      val (exp_group, exp_num) = exp_set
      val exp_frame = input_frame.filter( x => exp_group.contains(x._1) )
      //true is downstream, false is upstream
      //println(exp_frame.rowIx)

      val gene_list : Array[String] = if (cmdline.gene.isDefined) {
        Array(cmdline.gene())
      } else if (cmdline.genelist.isDefined) {
        fromFile(cmdline.genelist()).getLines().toArray.intersect( col_index.toSeq )
      } else {
        vertices.map( _._2.name ).intersect( col_index.toSeq )
      }

      println("Experiment Genes " + gene_list.mkString(","))

      //throw new Exception("testing")

      val experimentSet = exp_frame.
        flatMap(x => gene_list.map(y => (x._1, y, UPSTREAM)) ++ gene_list.map(y => (x._1, y, DOWNSTREAM))).collect()

      if (!Index(experimentSet).isUnique) {
        //make sure that duplicate experiments haven't been defined, this would cause problems later in the message intersection code
        println(experimentSet)
        throw new Exception("Duplicate item in index")
      }
      val experimentSet_br = sc.broadcast(experimentSet)

      val exp_value_frame = exp_frame.flatMap( x => x._2.map( y => (y._1, Map(x._1 -> y._2) ) ).toSeq ).
        reduceByKey(
       _ ++ _
      )


      //parallelize the matrix by the genes (rows) so the data can be attached to the graph
      //join the columns to the graph vertices by the vertex names
      val exp_rdd_joined = vertex_rdd.map(x => (x._2.name, x)).join(exp_value_frame).map(
        //create new vertex values, which are ShiftVertex structures
        x => (x._2._1._1, new ShiftVertex(x._2._1._2.name, x._2._1._2.nodeType, x._2._2))
      )

      //join the newly created ShiftVertexs with the existing graph structure
      var curGraph = graph.outerJoinVertices(exp_rdd_joined)(
        //joint the generated ShiftVertex structures to the graph, if a node isn't matched give it
        //an empty set of input values
        (x, y, z) => {
          z.getOrElse(new ShiftVertex(y.name, y.nodeType, Map()))
        }
      )

      var oldMessages : VertexRDD[List[ShiftMessage]] = null
      var oldGraph : Graph[ShiftVertex, SPFEdge ] = null
      curGraph = curGraph.mapVertices((x, y) => y.init(0.5, experimentSet_br.value))
      for (i <- 0 to cmdline.cycle_count()) {
        val experimentSet_br_local = experimentSet_br
        val curMessages = curGraph.aggregateMessages[List[ShiftMessage]](x => {
          //The edge passing rules go here
          //do a cycle over the different 'experiments'. Each experiment is different
          //gene to be knocked out

          if (x.srcAttr.levels != null) {
            //downstream messages
            val downstream = experimentSet_br_local.value.zipWithIndex.map( y => {
              //either the downstream target isn't part of the knockout experiment, or it isn't a upstream experiment
              if (y._1._2 != x.dstAttr.name || y._1._3 == UPSTREAM) {
                x.srcAttr.levels(y._2)
              } else {
                Double.NaN
              }
            })
            x.sendToDst(List(new ShiftMessage(x.attr.edgeType, DOWNSTREAM, downstream)))
          }
          if (x.dstAttr.levels != null) {
            //upstream messages
            val upstream = experimentSet_br_local.value.zipWithIndex.map( y => {
              //either the downstream target isn't part of the knockout experiment, or it isn't a downstream experiment
              if (y._1._2 != x.srcAttr.name || y._1._3 == DOWNSTREAM) {
                x.dstAttr.levels(y._2)
              } else {
                Double.NaN
              }
            })
            x.sendToSrc(List(new ShiftMessage(x.attr.edgeType, UPSTREAM, upstream)))
          }
        },
          (x, y) => {
            val out = x ++ y
            out
          }
        ).cache()
        curMessages.count()

        val l_inertia = if (i == 0)
          0.0
        else
          cmdline.intertia()
        val l_gravity = cmdline.gravity()
        val nextGraph = curGraph.outerJoinVertices(curMessages)(
          (vid, vertex, message) => (vertex, message.getOrElse(List[ShiftMessage]()))
        ).mapVertices( (x,y) => y._1.process( y._2, experimentSet_br_local.value, inertia=l_inertia, gravity=l_gravity ) ).cache()
        //curGraph.vertices.count()
        //curGraph.edges.count()
        if (oldGraph != null) {
          oldGraph.unpersistVertices(blocking = false)
          oldGraph.edges.unpersist(blocking = false)
        }
        if (oldMessages != null ) {
          oldMessages.unpersist(blocking=false)
        }
        oldMessages = curMessages
        oldGraph = curGraph
        curGraph = nextGraph

        if (cmdline.checkpoint.isDefined && i % cmdline.checkpoint() == 0) {
          curGraph.vertices.
            map(x => x._2.toJSON(experimentSet_br_local.value)).
            saveAsTextFile((outdir / ("set." + exp_num + ".cycle." + i + ".dump")).toString())
        }
      }
      //save the results
      curGraph.vertices.
        map(x => x._2.toJSON(experimentSet_br.value)).
        saveAsTextFile((outdir / ("set." + exp_num + ".final.dump")).toString())
      //unpersist all elements before the next cycle
      experimentSet_br.unpersist(blocking=true)
      oldGraph.unpersistVertices(blocking = true)
      oldGraph.edges.unpersist(blocking = true)
      if (oldMessages != null ) {
        oldMessages.unpersist(blocking=true)
      }
    })
  }
}

