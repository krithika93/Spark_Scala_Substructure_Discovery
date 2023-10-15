import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{LinkedHashMap, LinkedHashSet, WrappedArray}
import scala.util.control.Breaks._

object Partition extends  Serializable {


  //spark.conf.set("spark.dynamicAllocation.enabled", "true")
  //spark.conf.set("spark.executor.cores", 6)
  // spark.conf.set("spark.executor.instances", "4")
  //spark.conf.set("spark.dynamicAllocation.minExecutors", "4")
  //spark.conf.set("spark.dynamicAllocation.maxExecutors", "6")
  //val logData = spark.read.textFile(logFile).cache()
  val start = System.nanoTime()
  var no_of_iterations = 3;
  val conf = new SparkConf ().setAppName ("Graph Expansion")
  //System.setProperty("hadoop.home.dir", "C:\\")

  // sc is an existing SparkContext.

  /*
  parquet file reading at start
  Memory Serial only
  Map partitions when I need to only
  Broadcast variable for storing edges visited
  Map partitions to iterate over the array
  Errors in code was huge barrier
   */

  val spark = SparkSession.builder.appName ("Simple Application").master("local[*]").getOrCreate ()
  spark.sparkContext.setLogLevel("ERROR")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //val logFile = "README.md" // Should be some file on your system
  // val spark = SparkSession.builder.appName("Graph Expansion").getOrCreate()

  import spark.implicits._
  case class MyData(data : String,value:Array[String])

// broadcastCountries.value.get(country)
  def compareEdge(edgeInInstance: String, edgeToBeAdded: String): Int = {
    //Split by comma's
    val edI = edgeInInstance.split(",")
    val edA = edgeToBeAdded.split(",")
    val eII: String = edI(0) + "," + edI(2) + "," + edI(4) + "," + edI(1) + "," + edI(3)
    val eTA: String = edA(0) + "," + edA(2) + "," + edA(4) + "," + edA(1) + "," + edA(3)

    eII.compareTo(eTA);
  }

  def orderedexp(currEdge: Array[String], nedgeVfAdj: String): Array[String] = {
    var toBeEmitted: String = ""
    var insertFlag: Boolean = false
    var vedges = currEdge // current edge list from mapper
    for ((s, i) <- vedges.zipWithIndex) {
      breakable {
        if (insertFlag == true) {
          toBeEmitted = toBeEmitted + s + ";"
          break
        }
        var c: Int = compareEdge(s, nedgeVfAdj)
        // if c=0 strings are equal and will not be a case
        // if c > 0 s is lexicographically more than str
        // c < 0 otherwise
        if (c < 0) {
          toBeEmitted = toBeEmitted + s + ";"
        }
        else {
          toBeEmitted = toBeEmitted + nedgeVfAdj + ";" + s + ";"
          insertFlag = true
        }
      }
    }
    //all subs are checked
    //corner case the new edge to be placed at end
    if (insertFlag == false) {
      toBeEmitted = toBeEmitted + nedgeVfAdj + ";"
    }

    println("The emitted output from get ordered for    " + nedgeVfAdj)
    println(toBeEmitted)
    return toBeEmitted.split(";") // i have doubt in this code

  }


  def inputF() = {

    //val logData = spark.read.textFile(logFile).cache()
    val sc = spark.sparkContext

     //val input3 = sc.textFile("hdfs://cluster-6ae6-m-0/T10KV20KE_embed_5_vl_10_el_20_mr.g", 6).cache()
   // val input3 = sc.textFile("hdfs://cluster-6b7d-m/sample_graph.g", 18).cache()
    val input3 = sc.textFile("hdfs://cluster-6b7d-m/10k20k.g", 18).cache()
    case class MyData(data: String, value: Array[String])
    implicit val myObjEncoder1 = org.apache.spark.sql.Encoders.kryo[MyData]
    val in = spark.createDataset(input3)

    val rd = in.map(x => {

      val v1 = x.split(",")(1)
      val v3 = x.split(",")(3)
      var ver: IndexedSeq[String] = IndexedSeq(v1, v3)
      (ver, x)
    }).flatMap { case (x, y) => x.map((_, y)) }.groupByKey(x => x._1).mapValues(x => x._2).mapGroups((key, values) =>
      (key, values.toArray)).repartition($"_1").persist(StorageLevel.MEMORY_ONLY_SER)

    rd.write.mode(SaveMode.Overwrite).parquet("hdfs://cluster-6b7d-m/adjListfile2")
    //val adjList = rd.collect.toMap
    val adjList = sc.broadcast(rd.collect.toMap)

    rd.show()


    //val input = sc.textFile("hdfs://cluster-6b7d-m/fin1.txt", 6).cache()
    val input = sc.textFile("hdfs://cluster-6b7d-m/inputl/input.txt", 18).cache()
    val in1 = spark.createDataset(input)
/*
val lines1 = in1.map(x => {
  val vid = x.split("\t")(0)
  val e = x.split("\t")(1)
 List(vid, e.split(";"))
}).persist(StorageLevel.MEMORY_ONLY_SER) //or cache ??




 */
    val lines1 = in1.map(x => {
      val vid = x.split("\t")(0)
      val e = x.split("\t")(1)
      (vid, e.split(";"))
    }).persist(StorageLevel.MEMORY_ONLY_SER) //or cache ??
    lines1.write.mode(SaveMode.Overwrite).parquet("hdfs://cluster-6b7d-m/inputfile2")
    lines1.show()
    val relMap: LinkedHashMap[String, Integer] = LinkedHashMap()


    // val edg = lines2.repartition(6).map(el => el._2).cache() // contains all the visitededges
    //val visitv = sc.parallelize(visitedv)
    var strList: Array[String] = Array()
    //var toBeEmitted:Array[String]  = Array();

    // var larray = lines1.repartition(6).collect()

    //SECOND MAP

    val lines2 = lines1.select("_2").as[Array[String]].mapPartitions(x => {
      //var main_output: String = ""

      val e = x.next()

      //(vid, e.split(";"))

      val visE: LinkedHashSet[String] = LinkedHashSet[String]()
      val visitedv: LinkedHashSet[String] = LinkedHashSet[String]()
      for (ee: String <- e) { // similar to Mapmethod starting

        visE.add(ee.toString)
      }
      List(visE.toSet).iterator
    })
    val subs_instances = lines2.mapPartitions(x => {

      val e = x.next()
      var main_output =""
      val visitedv: LinkedHashSet[String] = LinkedHashSet[String]()
      for (ee: String <- e) { // similar to Mapmethod starting
        var vertices: Array[String] = ee.split(",")
        var vertex = vertices.toList
        for (i <- 1 to vertices.size - 1 by 2) {
          visitedv.add(vertex(i))
        }
      }
      for (v <- visitedv) {

        //var edges1 = adjList.get(v).head
        var edges1 = adjList.value.get(v).get
        edges1.foreach(println)
        var edgeTobeExpanded = e

        if (no_of_iterations > 0) {
          for (s <- edges1) {

            if (!e.contains(s)) {
              no_of_iterations = no_of_iterations - 1;

              println(s + "   this edge not present ")
              println(edgeTobeExpanded.mkString(",") + "This edges is current input to function")

              var toBeEmitted: Array[String] = orderedexp(edgeTobeExpanded.asInstanceOf[Array[String]], s)
              println("toBeEmitted")
              toBeEmitted.foreach(println)
              var cnt: Int = 1;
              var output: String = ""
              var oneEdge: Array[String] = Array()
              var count: Int = 0
              //we have to create the canonical substructure now
              var label: String = ""
              for (edge <- toBeEmitted) {
                oneEdge = edge.split(",")
                breakable {
                  if (oneEdge.size != 5) {
                    break;
                  }
                  if (!relMap.contains(oneEdge(1))) {
                    println(count + "      " + cnt)
                    relMap.put(oneEdge(1), cnt) //putting the vertices in the relMap in the canonical order
                    cnt = cnt + 1
                  }
                  if (!relMap.contains(oneEdge(3))) {
                    println(count + "      " + cnt)
                    relMap.put(oneEdge(3), cnt)
                    cnt = cnt + 1
                  }
                  //creating the k-edge substructure
                  label = label + oneEdge(0) + "," + oneEdge(2) + "," + oneEdge(4) + "," + relMap.get(oneEdge(1)).head + "," + relMap.get(oneEdge(3)).head + ";"
                  println(label + "     " + count + "   cnt in relMap " + cnt)
                  count = count + 1
                }
              }
              println("Ed vs Emitted " + "     " + toBeEmitted.mkString(";"))
              toBeEmitted.foreach(println)
              var ed = toBeEmitted.mkString(";")
              output = label + "\t" + ed + "\n";
              println("OUTPUT")
              println(output)
              main_output = main_output + output;
              output = ""
              relMap.empty
            }
          }
        }
      }
      visitedv.empty
      if(main_output != "") {
        List(main_output.split("\n")).iterator
      }
      else
        {
          List().iterator
        }
      })

    //SECOND MAP
/*
    val lines3 = lines1.select("_2").as[Array[String]].map(x => {
      var main_output: String = ""

      val e= x

      //(vid, e.split(";"))

      val visE: LinkedHashSet[String] = LinkedHashSet[String]()
      val visitedv: LinkedHashSet[String] = LinkedHashSet[String]()
      for (ee:String <- e) { // similar to Mapmethod starting

        visE.add(ee.toString)
        var vertices: Array[String] = ee.split(",")

        var vertex = vertices.toList
        for (i <- 1 to vertices.size - 1 by 2) {
          visitedv.add(vertex(i))
        }
      }
      visitedv
      for (v <- visitedv) {

        //var edges1 = adjList.get(v).head
        var edges1 = adjList.value.get(v).get
        edges1.foreach(println)
        var edgeTobeExpanded = e

        if (no_of_iterations > 0) {
          for (s <- edges1) {

            if (!visE.contains(s)) {
              no_of_iterations = no_of_iterations - 1;

              println(s + "   this edge not present ")
              println(edgeTobeExpanded.mkString(",") + "This edges is current input to function")

              var toBeEmitted: Array[String] = orderedexp(edgeTobeExpanded.asInstanceOf[Array[String]], s)
              println("toBeEmitted")
              toBeEmitted.foreach(println)
              var cnt: Int = 1;
              var output: String = ""
              var oneEdge: Array[String] = Array()
              var count: Int = 0
              //we have to create the canonical substructure now
              var label: String = ""
              for (edge <- toBeEmitted) {
                oneEdge = edge.split(",")
                breakable {
                  if (oneEdge.size != 5) {
                    break;
                  }
                  if (!relMap.contains(oneEdge(1))) {
                    println(count + "      " + cnt)
                    relMap.put(oneEdge(1), cnt) //putting the vertices in the relMap in the canonical order
                    cnt = cnt + 1
                  }
                  if (!relMap.contains(oneEdge(3))) {
                    println(count + "      " + cnt)
                    relMap.put(oneEdge(3), cnt)
                    cnt = cnt + 1
                  }
                  //creating the k-edge substructure
                  label = label + oneEdge(0) + "," + oneEdge(2) + "," + oneEdge(4) + "," + relMap.get(oneEdge(1)).head + "," + relMap.get(oneEdge(3)).head + ";"
                  println(label + "     " + count + "   cnt in relMap " + cnt)
                  count = count + 1
                }
              }

              println("Ed vs Emitted " + "     " + toBeEmitted.mkString(";"))
              toBeEmitted.foreach(println)
              var ed = toBeEmitted.mkString(";")
              output = label + "\t" + ed + "\n";
              println("OUTPUT")
              println(output)
              main_output = main_output + output;
              output = ""
              relMap.empty
            }
          }
        }
      }
      visE.empty
      visitedv.empty
      main_output
      }) */
    val lines3 = subs_instances.filter(_.size != 0).select(explode($"value")).map(x => {
      var xx =  x.toString.split("\t")
      (xx(0).toString, xx(1).toString)
    })

    lines3.show()
    //subs3.foreach(println)
    //Reducer Code
    lines2.unpersist()
    lines1.unpersist()
    rd.unpersist()
    var beamMap: LinkedHashMap[Double, LinkedHashSet[String]] = LinkedHashMap()
    var adjacencyMap: LinkedHashMap[Double, LinkedHashSet[String]] = LinkedHashMap()


    case class Group(Substructure: String,
                     instances: Array[String])
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Group]

    val visitedNodes: LinkedHashSet[String] = LinkedHashSet[String]()
    val nonOverlapSubs: LinkedHashSet[String] = LinkedHashSet[String]() // non overlapping
    var dupRemovedInstances: LinkedHashSet[String] = LinkedHashSet()


    //kryo(classOf[Group])
    //val groupExprEncoder = groupEncoder.asInstanceOf[ExpressionEncoder[Group]]
    // val enc: ExpressionEncoder[List[Group]] = ExpressionEncoder()
    //val enc: Encoder[ListBuffer[Mod]] = ExpressionEncoder()
    val subs3 = lines3.groupByKey(x => x._1).mapValues(x => x._2).mapGroups((key, values) =>
      (key, values.toArray)).repartition($"_1").persist(StorageLevel.MEMORY_AND_DISK_SER)
    val keys = subs3.select($"_1").cache()
    val values = subs3.select($"_2").cache()
    //subs3.show()
    subs3.schema
    subs3.write.mode(SaveMode.Overwrite).parquet("hdfs://cluster-6b7d-m/group3")
    var noOfVertices: Int = 0
    var noOfEdges: Int = 0


    for (o <- keys.first.get(0).toString.split(";")) {
      var subs: Array[String] = o.split(",")
      if (subs.length == 5 && subs(3).forall(Character.isDigit) && subs(4).forall(Character.isDigit)) {

        if ( Integer.parseInt(subs(3)) >= noOfVertices) {
          noOfVertices = Integer.parseInt(subs(3))
          noOfEdges = noOfEdges + 1

        }
        if (Integer.parseInt(subs(4)) >= noOfVertices) {
          noOfVertices = Integer.parseInt(subs(4))
          noOfEdges = noOfEdges + 1
        }
      }
    }
    for (v <- values.first.getAs[WrappedArray[String]](0).asInstanceOf[Array[String]]) {
      val instances = v
      // list of all substructure Instances associated with substructure {
      //find vertices on subINstance
      //if you find another sub with overlapped vertices ignore it
      var subVList = v.split(",")
      //var l =
      var count: Integer = 0

      for (i <- 1 until subVList.length by 2) {
        breakable {
          var sub = subVList(i)

          if (visitedNodes.contains(sub)) {
            break
          }
          count = count + 1
        }
      }
      //System.out.println("Count = "+count+ " and key count = "+2*val.toString().split(";").length);
      if (count == 2 * v.split(";").length) { //if the number of vertices is twice of length of v
        nonOverlapSubs.add(v)

        for (i <- 1 until subVList.length by 2) {
          var el = subVList(i)
          visitedNodes.add(el)
        }
      }
    }

    var files: Array[String] = Array[String]()

    FileSystem.get(sc.hadoopConfiguration).listStatus(new Path("hdfs://cluster-6b7d-m/group3/")).foreach(x => {
      println(x.getPath);
      if (x.getPath.toString != "hdfs://cluster-6b7d-m/group3/_SUCCESS") {
        files = files :+ x.getPath.toString;
      }
    })
   // println(files)
    //println("Checking whether file is empty", files.size)

    var dupTimeEnd: Long = System.currentTimeMillis()
    val t0 = System.currentTimeMillis()
    //nonOverlapSubs now contains all substructures
    var noOfInstances = nonOverlapSubs.size
    var num1 = 5 - (noOfInstances * noOfVertices) + noOfInstances //INPUT_GRAPH_VERTEX
    var num2 = 6 - (noOfInstances * noOfEdges) //INPUT_GRAPH_EDGE
    var num3:Int = noOfEdges + noOfVertices
    var dec1 = 5 + 6; //INPUT_GRAPH_EDGE+INP
    var mdl: Double = (num1 + num2 + num3) / dec1
    println(mdl)
    if (beamMap.keySet.size < 2) {
      if (beamMap.contains(mdl)) {

        var m = beamMap.get(mdl); // There will be many mdls, it returns like option

        var m1 = beamMap.get(mdl).head

        if (m1.isInstanceOf[LinkedHashSet[String]]) {
          // println("addition operation taking place")
          m1 = m1 ++ nonOverlapSubs

          beamMap.remove(mdl)
          beamMap.put(mdl, m1)
        }
      }

      else {
        //  println("addition operation taking place")

        beamMap.put(mdl, nonOverlapSubs)
      }
    }
    //var kv = a.zipAll(b,a(0), b(0));
    //var kv1= l.map(i => i._1 -> i._2).toMap;
    else {
      var max: Double = 0.0
      for (d <- beamMap.keySet) {
        if (max <= d) {
          max = d;
        }
      }
      //now I have found my max mdl so check current mdl with max mdl
      if (mdl > max) {
        //remove the max from beamMap
        beamMap.remove(max);
        beamMap.put(mdl, nonOverlapSubs)
      }
    }
    var res: Array[String] = Array()
    beamMap.foreach { x => res = res :+ (x._1.toString + "\t" + x._2.mkString(";") + "\n") }
    println("BeamMap")
    beamMap.foreach(println)

    spark.createDataset(res).write.mode(SaveMode.Append).mode(SaveMode.Overwrite).text("hdfs://cluster-6b7d-m/final_beam")

    val duration = (System.nanoTime - start) / 1e9d
    println("elapsedTimeInSecond by Mapper  " + duration)





/*
    for (f <- files) {
      var visitedNodes: LinkedHashSet[String] = LinkedHashSet[String]()
      var nonOverlapSubs: LinkedHashSet[String] = LinkedHashSet[String]() // non overlapping
      var dupRemovedInstances: LinkedHashSet[String] = LinkedHashSet()
      var noOfVertices: Integer = 0
      var noOfEdges: Integer = 0
      val ds = spark.read.parquet(f).select("_1")
      val ds1 = spark.read.parquet(f).select("_2")
      //val ds = spark.read.format("parquet").load(path=f)
      //val ds1 = spark.read.load(path = f, format = "parquet")
      if (ds.count > 0) {
        var instances_v = ds1.first.getAs[WrappedArray[String]](0).mkString("\n").split("\n")
       // var instances_v1= ds1.first.getAs[WrappedArray[String]](0)
      // var instances_v1= ds1.first.getAs[WrappedArray[String]](0)
        var sub_k = ds.first.get(0).toString.split(";")
        println(instances_v.size)
        println(sub_k.size)
        //for( (x,i) <- instances_v.zipWithIndex)  { sub_k.foreach(println); println( " instances      "+x +"   "+i) }
        // after expansion with adjacency matrix
        for (o <- sub_k) {
          var subs: Array[String] = o.split(",");
          if (subs.length == 5 && subs(3).forall(Character.isDigit) && subs(4).forall(Character.isDigit)) {

            if (noOfVertices <= Integer.parseInt(subs(3))) {
              noOfVertices = Integer.parseInt(subs(3));
              noOfEdges = noOfEdges + 1;
            }
            if (noOfVertices <= Integer.parseInt(subs(4))) {
              noOfVertices = Integer.parseInt(subs(4));
              noOfEdges = noOfEdges + 1;
            }
          }
        }

        for (v <- instances_v) {
          // list of all substructure Instances associated with substructure {
          //find vertices on subINstance
          //if you find another sub with overlapped vertices ignore it
          var subVList = v.split(",")
          //var l =
          var count: Integer = 0

          for (i <- 1 until subVList.length by 2) {
            breakable {
              var sub = subVList(i)

              if (visitedNodes.contains(sub)) {
                break
              }
              count = count + 1
            }
          }
          //System.out.println("Count = "+count+ " and key count = "+2*val.toString().split(";").length);
          if (count == 2 * v.split(";").length) {   //if the number of vertices is twice of length of v
            nonOverlapSubs.add(v)

            for (i <- 1 until subVList.length by 2) {
              var el = subVList(i)
              visitedNodes.add(el)
            }
          }
        }

        var dupTimeEnd: Long = System.currentTimeMillis()
        val t0 = System.currentTimeMillis()
        //nonOverlapSubs now contains all substructures
        var noOfInstances = nonOverlapSubs.size
        var num1 = 5 - (noOfInstances * noOfVertices) + noOfInstances //INPUT_GRAPH_VERTEX
        var num2 = 6 - (noOfInstances * noOfEdges) //INPUT_GRAPH_EDGE
        var num3 = noOfEdges + noOfVertices
        var dec1 = 5 + 6; //INPUT_GRAPH_EDGE+INP
        var mdl: Double = (num1 + num2 + num3) / dec1
        println(mdl)
        if (beamMap.keySet.size < 2) {
          if (beamMap.contains(mdl)) {

            var m = beamMap.get(mdl); // There will be many mdls, it returns like option

            var m1 = beamMap.get(mdl).head

            if (m1.isInstanceOf[LinkedHashSet[String]]) {
              // println("addition operation taking place")
              m1 = m1 ++ nonOverlapSubs

              beamMap.remove(mdl)
              beamMap.put(mdl, m1)
            }
          }

          else {
            //  println("addition operation taking place")

            beamMap.put(mdl, nonOverlapSubs)
          }
        }
        //var kv = a.zipAll(b,a(0), b(0));
        //var kv1= l.map(i => i._1 -> i._2).toMap;
        else {
          var max: Double = 0.0
          for (d <- beamMap.keySet) {
            if (max <= d) {
              max = d;
            }
          }
          //now I have found my max mdl so check current mdl with max mdl
          if (mdl > max) {
            //remove the max from beamMap
            beamMap.remove(max);
            beamMap.put(mdl, nonOverlapSubs)
          }
        }
        var res: Array[String] = Array()
        beamMap.foreach { x => res = res :+ (x._1.toString + "\t" + x._2.mkString(";") + "\n") }
        println("BeamMap")
        beamMap.foreach(println)

        spark.createDataset(res).write.mode(SaveMode.Append).mode(SaveMode.Overwrite).text("hdfs://cluster-6b7d-m/final_beam")

        val duration = (System.nanoTime - start) / 1e9d
        println("elapsedTimeInSecond by Mapper  " + duration)
      }
    }

    */

  }


    // val res = ds.select("_2").first.getAs[WrappedArray[String]](0).mkString("\n").split("\n")


    // subs3.saveAsTextFile(" hdfs://cluster-6ae6/output_spark/subs_mapper")
    // //REDUCER CODE

    //val groupExprEncoder = TypedEncoder.asInstanceOf[]
  /*  implicit  val en = TypedEncoder.asInstanceOf[TypedEncoder[Group]]
    implicit val s = TypedEncoder.asInstanceOf[TypedEncoder[Any]]*/
/*
    val ds = spark.read.parquet("hdfs://cluster-6ae6/group").as[Group]
    ds.select($"Substructure").show(100)
    ds.select($"Substructure").collect.foreach( x => {

      var oneSub: Array[String] = x.toString().split(";"); // s is the substructure, si is the substructure instance
      // after expansion with adjacency matrix
      for (o <- oneSub) {
        var subs: Array[String] = o.split(",");
        if (subs.length == 5 && subs(3).forall(Character.isDigit) && subs(4).forall(Character.isDigit)) {

          if (noOfVertices <= Integer.parseInt(subs(3))) {
            noOfVertices = Integer.parseInt(subs(3));
            noOfEdges = noOfEdges + 1;
          }
          if (noOfVertices <= Integer.parseInt(subs(4))) {
            noOfVertices = Integer.parseInt(subs(4));
            noOfEdges = noOfEdges + 1;
          }
        }
      }
    }
    )
*/
    //ds.select($"instances").collect



  //for( el <- lines1.collect())  { val vid = el._1; val e = el._2; adM.put(vid,e); } // already stored in adjacent list basedon

  def main(args: Array[String]): Unit = {
    var mapperStartTime = System.nanoTime()
    inputF()
    //sc.parallelize(mapperTotalTime).saveAsTextFile("krithika/time")
    val duration = (System.nanoTime - mapperStartTime) / 1e9d
    println("elapsedTimeInSecond by Mapper  " + duration)

  }

}