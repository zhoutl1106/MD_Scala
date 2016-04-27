import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.spark.rdd.RDD

/**
  * Created by zhoutianli on 4/24/16.
  */

import scala.collection.mutable.Seq

class Atom extends Serializable {
  var Element: String = ""
  var x: Double = 0.0
  var y: Double = 0.0
  var z: Double = 0.0

  def print(): Unit = {
    println(Element + " : (" + x + "," + y + "," + z + ")")
  }
}

object MD_Scala {
  var m_file: String = ""
  var dat: Array[Array[(String, Double, Double, Double)]] =
    Array.empty[Array[(String, Double, Double, Double)]]
  var sc: SparkContext = null
  var isCluster = false
  var lastRunningTime: Array[Double] = Array(-1.0, -1.0, -1.0, -1.0)

  def Set_Spark_Context(sc1: SparkContext): Unit = {
    sc = sc1
  }

  def Set_Cluster(is : Boolean) : Unit = isCluster = is

  def Get_Last_Running_Time: Array[Double] = lastRunningTime

  def Load_XYZ_File(patten: String, file: String): Unit = {
    m_file = file
    var content = ""

    // use Spark Context to open file, which is the only way I
    // know that could handle both local file and HDFS
    content = sc.textFile(m_file).collect().mkString("\n")

    // Split to time frames with pattern
    // it's "number of atoms, comment" on start of
    // each time frame in XYZ file
    val frames = content.split(patten).filter(_ != "")
    dat = frames.map(p => {
      val t = p.split("\n").filter(_ != "");
      t.map(e => {
        val temp = e.split(" +").filterNot(_ == "")
        (temp(0), temp(1).toDouble, temp(2).toDouble, temp(3).toDouble)
      })
    })
  }

  def Get_Kurtosis_of_Pair(p0: Int, p1: Int, method: Int): Double = {
    val v = ArrayBuffer.empty[(Atom, Atom)]
    val loop = dat.size - 1
    var ret: Double = 0.0
    lastRunningTime(0) = -1.0
    lastRunningTime(1) = -1.0
    lastRunningTime(2) = -1.0
    lastRunningTime(3) = -1.0
    var t1 = System.nanoTime()

    // Generate Array of atom pairs
    for (i <- 0 to loop) {
      val a0 = Get_Atom(i, p0)
      val a1 = Get_Atom(i, p1)
      v += Tuple2(a0, a1)
    }
    var t2 = System.nanoTime()
    lastRunningTime(0) = (t2 - t1) / 1e6
    t1 = t2

    val vv = v.toArray
    t2 = System.nanoTime()
    lastRunningTime(1) = (t2 - t1) / 1e6
    t1 = t2

    // generate distance array
    // then calculate kurtosis on it.
    if (isCluster) {
      val temp = Cluster_Distance(vv)
      t2 = System.nanoTime()
      lastRunningTime(2) = (t2 - t1) / 1e6
      t1 = t2
      if (method > 0)
        ret = Cluster_Kurtosis(temp)
      else
        ret = Cluster_Online_Kurtosis(temp)
      t2 = System.nanoTime()
      lastRunningTime(3) = (t2 - t1) / 1e6
      t1 = t2
    }
    else {
      val arr = (0 to loop).map(k => {
        Standalone_Distance(vv(k)._1, vv(k)._2)
      })
      t2 = System.nanoTime()
      lastRunningTime(2) = (t2 - t1) / 1e6
      t1 = t2
      if (method > 0)
        ret = Standalone_Kurtosis(arr.toArray)
      else
        ret = Standalone_Online_Kurtosis(arr.toArray)
      t2 = System.nanoTime()
      lastRunningTime(3) = (t2 - t1) / 1e6
      t1 = t2
    }
    ret
  }

  def Get_Atom(time: Int, pos: Int): Atom = {
    val a = new Atom
    a.Element = dat(time)(pos)._1
    a.x = dat(time)(pos)._2
    a.y = dat(time)(pos)._3
    a.z = dat(time)(pos)._4
    a
  }

  def Cluster_Distance(a0: Array[(Atom, Atom)]): RDD[Double] = {

    val p = sc.parallelize(a0)
    p.map(e => {
      Math.sqrt((e._1.x - e._2.x) * (e._1.x - e._2.x) +
        (e._1.y - e._2.y) * (e._1.y - e._2.y) +
        (e._1.z - e._2.z) * (e._1.z - e._2.z))
    })
  }

  def Standalone_Distance(a0: Atom, a1: Atom): Double = {
    Math.sqrt((a0.x - a1.x) * (a0.x - a1.x) +
      (a0.y - a1.y) * (a0.y - a1.y) +
      (a0.z - a1.z) * (a0.z - a1.z))
  }

  def Standalone_Kurtosis(data: Array[Double]): Double = {
    val c = data
    val cnt = c.length
    val mean = c.sum / cnt
    val devs = c.map(rd => (rd - mean) * (rd - mean))
    val stddev = Math.sqrt(devs.sum / cnt)

    val expect = c.map(rd => (rd - mean) * (rd - mean) * (rd - mean) * (rd - mean))
    val kurtosis = expect.sum / cnt / Math.pow(stddev, 4)

    kurtosis
  }

  def Standalone_Online_Kurtosis(data: Array[Double]): Double = {
    // it's the Online Incremental Algorithm from wikipedia
    var n = 0.0
    var mean = 0.0
    var M2 = 0.0
    var M3 = 0.0
    var M4 = 0.0

    data.foreach(x => {
      val n1 = n
      n = n + 1
      val delta = x - mean
      val delta_n = delta / n
      val delta_n2 = delta_n * delta_n
      val term1 = delta * delta_n * n1
      mean = mean + delta_n
      M4 = M4 + term1 * delta_n2 * (n * n - 3 * n + 3) + 6 * delta_n2 * M2 - 4 * delta_n * M3
      M3 = M3 + term1 * delta_n * (n - 2) - 3 * delta_n * M2
      M2 = M2 + term1
    })

    (n * M4) / (M2 * M2)
  }

  def Cluster_Kurtosis(data: RDD[Double]): Double = {
    val cnt = data.count()
    val mean = data.sum() / cnt
    val devs = data.map(rd => (rd - mean) * (rd - mean))
    val stddev = Math.sqrt(devs.sum() / cnt)

    val expect = data.map(rd => (rd - mean) * (rd - mean) * (rd - mean) * (rd - mean))
    val kurtosis = expect.sum() / cnt / Math.pow(stddev, 4)

    kurtosis
  }

  def Cluster_Online_Kurtosis(data: RDD[Double]): Double = {
    var n = 0.0
    var mean = 0.0
    var M2 = 0.0
    var M3 = 0.0
    var M4 = 0.0

    val temp = data.map(x => {
      val n1 = n
      n = n + 1
      val delta = x - mean
      val delta_n = delta / n
      val delta_n2 = delta_n * delta_n
      val term1 = delta * delta_n * n1
      mean = mean + delta_n
      Vectors.dense(term1 * delta_n2 * (n * n - 3 * n + 3) + 6 * delta_n2 * M2 - 4 * delta_n * M3,
        term1 * delta_n * (n - 2) - 3 * delta_n * M2,
        term1)
    })

    val summary: MultivariateStatisticalSummary = Statistics.colStats(temp)
    val means = summary.mean

    means(0) / (means(2) * means(2))
  }

}
