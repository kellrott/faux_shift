package edu.ucsc

import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

import scala.collection.mutable.ArrayBuffer

object SPFVertex {
  def apply(i:String) : SPFVertex = {
    new SPFVertex(i, null)
  }
}

class SPFVertex(val name : String, val nodeType : String) extends Serializable {
  override def equals(a: Any) : Boolean = {
    return a.isInstanceOf[SPFVertex] && a.asInstanceOf[SPFVertex].name == name
  }
  override def hashCode() : Int = {
    return name.hashCode()
  }
  override def toString() : String = {
    return "%s(%s)".format(name, nodeType)
  }
}


object SPFEdge {
  def apply(i:String) : SPFEdge = {
    new SPFEdge(i)
  }
}

class SPFEdge(val edgeType : String) extends Serializable {

}

object SPFReader {

  def read(path: String) : (Array[(Long,SPFVertex)], Array[(Long,Long,SPFEdge)]) = {
    val out_vertex = new ArrayBuffer[(Long,SPFVertex)]()
    val out_edge = new ArrayBuffer[(Long,Long,SPFEdge)]()
    val id_map = new HashMap[SPFVertex,Long]()

    fromFile(path).getLines().foreach( x => {
      val line = x.split("\t")
      if (line.length == 2) {
        val v = new SPFVertex(line(1), line(0))
        if (!id_map.contains(v)) {
          id_map(v) = id_map.size
        }
        out_vertex.append( (id_map(v),v) )
      } else if (line.length == 3)  {
        val vs = SPFVertex(line(0))
        if (!id_map.contains(vs)) {
          id_map(vs) = id_map.size
        }
        val vd = SPFVertex(line(1))
        if (!id_map.contains(vd)) {
          id_map(vd) = id_map.size
        }
        out_edge.append( (id_map(vs), id_map(vd), SPFEdge(line(2)) ) )
      }
    })
    (out_vertex.toArray, out_edge.toArray)
  }
}