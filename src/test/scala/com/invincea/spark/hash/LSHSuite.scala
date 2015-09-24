package com.invincea.spark.hash

import org.apache.spark.rdd.RDD

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{SparseVector, Vectors}

class LSHSuite extends FunSuite with LocalSparkContext {
  test("simple test") {

    
    val data = List(List(21, 25, 80, 110, 143, 443),
                    List(21, 25, 80, 110, 143, 443, 8080),
                    List(80, 2121, 3306, 3389, 8080, 8443),
                    List(13, 17, 21, 23, 80, 137, 443, 3306, 3389))
                    
    val rdd = sc.parallelize(data)
    
    //make sure we have 4
    assert(rdd.count() == 4)
    
    val vctr = rdd.map(r => (r.map(i => (i, 1.0)))).map(a => Vectors.sparse(65535, a).asInstanceOf[SparseVector])
    
    //make sure we still have 4
    assert(vctr.count() == 4)
    
    
    val lsh = new  LSH(data = vctr, p = 65537, m = 1000, numRows = 1000, numBands = 100, minClusterSize = 2)
    val model = lsh.run

    //this should return 1 cluster
    assert(model.clusters.count() == 1)
    
    //filter for clusters with jaccard score > 0.85. should still return 1    
    assert(model.filter(0.5).clusters.count() == 1)
    
  }

  test("with clique data"){
    val rawData = sc.textFile("hdfs://127.0.0.1:9000/spark/input/cliqueData")
      .map{ line =>
       val fields = line.replace("(", "").replace(")", "").replace("idx","").split(",")
      val res = fields(2).trim.split(" ").map(str => str.toInt)
      res.toList
    }.distinct().cache()

//    val maxElm = rawData.map(e => e.reduce(Math.max(_, _))).reduce(Math.max(_, _))
//    println("max element: " + maxElm)     //maxElm = 12515600


    val vctr = rawData.map(r => (r.map(i => (i, 1.0)))).map(a => Vectors.sparse(12515601, a).asInstanceOf[SparseVector])

    val lsh = new  LSH(data = vctr, p = 12515603, m = 1000, numRows = 1000, numBands = 100, minClusterSize = 2)
    val model = lsh.run
    model.filter(0.85)
    model.clusters.saveAsTextFile("hdfs://127.0.0.1:9000/spark/output/LSH_cluster")
    model.cluster_vector.saveAsTextFile("hdfs://127.0.0.1:9000/spark/output/LSH_cluster2vector")

    println("clusters: " + model.clusters.count())

  }

  test("find the next prime number"){
    println(Prime.is(9))
  }


  def saveRDDAsTxt[T](rdd: RDD[T], p: String): Unit = {
    try {
      val path = scala.reflect.io.Path(p)
      path.deleteRecursively()
    } catch {
      case _: Throwable => println("some file could not be deleted")
    }

    rdd.take(1)    //todo: for debug purpose
    rdd.saveAsTextFile(p)
  }

  object Prime {
    def is(i: Long) =
      if (i == 2) true
      else if ((i & 1) == 0) false // efficient div by 2
      else prime(i)

    def primes: Stream[Long] = 2 #:: prime3

    private val prime3: Stream[Long] = {
      @annotation.tailrec
      def nextPrime(i: Long): Long =
        if (prime(i)) i else nextPrime(i + 2) // tail

      def next(i: Long): Stream[Long] =
        i #:: next(nextPrime(i + 2))

      3 #:: next(5)

    }

    // assumes not even, check evenness before calling - perf note: must pass partially applied >= method
    def prime(i: Long) =
      prime3 takeWhile (math.sqrt(i).>= _) forall { i % _ != 0 }
  }
}