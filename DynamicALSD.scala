package src.main.scala
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util._

import java.io._
import java.net.URI
import scala.collection.mutable.{LinkedHashMap, LinkedHashSet}
import scala.util.control.Breaks._

object DynamicALSD {


  class GraphMap extends Mapper[Text, Text, Text, Text] {

    //var one: IntWritable = new IntWritable(1);
    //var word: Text = new Text();


    //var one: IntWritable = new IntWritable(1);
    //var word: Text = new Text();


    var adjacencyMap: LinkedHashMap[String, LinkedHashSet[String]] = LinkedHashMap()
    var edgeVals: LinkedHashSet[String] = LinkedHashSet()
    // What is called in scal
    var visitedNodes: LinkedHashSet[String] = LinkedHashSet()
    var visitedEdges: LinkedHashSet[String] = LinkedHashSet()
    var str: String = ""
    var edges: LinkedHashSet[String] = LinkedHashSet()
    var keyVal: Array[String] = Array[String]()
    var relMap: LinkedHashMap[String, Integer] = LinkedHashMap()


    def compareEdge(edgeInInstance: String, edgeToBeAdded: String): Int = {
      //Split by comma's
      val edI = edgeInInstance.split(",")
      val edA = edgeToBeAdded.split(",")
      val eII: String = edI(0) + "," + edI(2) + "," + edI(4) + "," + edI(1) + "," + edI(3)
      val eTA: String = edA(0) + "," + edA(2) + "," + edA(4) + "," + edA(1) + "," + edA(3)

      eII.compareTo(eTA)
    }

    def orderedexp(currEdge: Array[String], nedgeVfAdj: String): Array[String] = {
      var toBeEmitted: String = ""
      var insertFlag: Boolean = false
      val edges = currEdge // current edge list from mapper
      for ((s, i) <- edges.zipWithIndex) {
        breakable {
          if (insertFlag) {
            toBeEmitted = toBeEmitted + s + ";"
            break;
          }
          val c: Int = compareEdge(s, nedgeVfAdj);
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
      if (!insertFlag) {
        toBeEmitted = toBeEmitted + nedgeVfAdj + ";"
      }

      println("The emitted output from get ordered for    " + nedgeVfAdj)
      println(toBeEmitted)
      toBeEmitted.split(";") // i have doubt in this code

    }

    override def setup(context: Mapper[Text, Text, Text, Text]#Context) {
      try {
        var str: String = null
        val cacheFiles: Array[URI] = context.getCacheFiles

        if (cacheFiles != null && cacheFiles.length > 0) {


          var line: String = "";
          val fs: FileSystem = FileSystem.get(context.getConfiguration)
          val pt: Path = new Path(cacheFiles(0).toString)
          val br = new BufferedReader(new InputStreamReader(fs.open(pt)))

          var str1: String = br.readLine


          while (str1 != null) {
            //keyVal = str.split("[\t]")
            //val vId = keyVal(0)
            edges.add(str1)
            str1 = br.readLine
          }


          val s = edges.groupBy(x => x.split(",")(1))
          val d = edges.groupBy(x => x.split(",")(3))
          adjacencyMap = adjacencyMap ++ s
          adjacencyMap = adjacencyMap ++ d


        }
      }
      catch {
        case e: IOException => println("Unable to Read File")
      }
    }

    override def map(key: Text, value: Text, context: Mapper[Text, Text, Text, Text]#Context) {

      var edgeVals: LinkedHashSet[String] = LinkedHashSet()
      // What is called in scal
      var visited: LinkedHashSet[String] = LinkedHashSet()
      var visE: LinkedHashSet[String] = LinkedHashSet()
      var str: String = ""
      val edges: LinkedHashSet[String] = LinkedHashSet()
      var keyVal = Array[String]()
      val relMap: LinkedHashMap[String, Integer] = LinkedHashMap()
      val keyT = new Text()
      val valT = new Text()


      if (key.toString.equals("Partition")) {
        try {
          var adjLoadStartTime = System.nanoTime
          val cacheFiles: Array[URI] = context.getCacheFiles()
          if (cacheFiles != null && cacheFiles.length > 0) {

            val fs: FileSystem = FileSystem.get(context.getConfiguration)
            val pt: Path = new Path(cacheFiles(0).toString)
            val br = new BufferedReader(new InputStreamReader(fs.open(pt)))

            var str: String = br.readLine


            while (str != null) {
              //keyVal = str.split("[\t]")
              //val vId = keyVal(0)
              edges.add(str)
              str = br.readLine
            }


            val s = edges.groupBy(x => x.split(",")(1))
            val d = edges.groupBy(x => x.split(",")(3))
            adjacencyMap = adjacencyMap ++ s
            adjacencyMap = adjacencyMap ++ d
            var adjLoadEndTime = System.nanoTime
            //expansionStartTime = System.nanoTime
          }
        }
        catch {
          case e: Exception =>
            throw new RuntimeException(e) // bizarre!

        }

        //var a: String = "";

        context.write(new Text("Partition"), new Text("Partition"))

      }

      //   }
      else if (!key.toString.equals("Partition")) {

        var strList: Array[String] = Array[String]()
        strList = value.toString.split(",")

        if (strList.length < 1) {
          break
        }
        println(value)
        var s: String = ""

        for (ee <- value.toString.split(";")) {
          visE.add(ee)

          // context.write(new Text(s), new Text(value.toString))

        }
        val v: String = ""

        for (i <- 1 to strList.length - 1 by 2) {
          visited.add(strList(i))


        }
        for (v: String <- visited) {
          val edges1 = adjacencyMap.get(v).toList.head

          //var edges1 = adjacencyMap.getOrElse(v,0)
          // if ( edges1 == 0 ) { break }
          //  else { edges1 = edges1. asInstanceOf[HashSet[String]] }
          //edges1.foreach(println)
          for (s <- edges1) {
            if (!visE.contains(s)) {

              //println("Expanding va"+value+ "by adding"+s)
              val toBeEmitted: Array[String] = orderedexp(value.toString.split(";"), s)
              toBeEmitted.foreach(println)
              var cnt: Int = 1
              var output: String = ""
              var oneEdge: Array[String] = Array()

              //we have to create the canonical substructure now
              var label: String = ""
              for (edge <- toBeEmitted) {
                oneEdge = edge.split(",") // error

                if (oneEdge.length != 5) {
                  break
                }
                if (!relMap.contains(oneEdge(1))) {
                  println(cnt)
                  relMap.put(oneEdge(1), cnt) //putting the vertices in the relMap in the canonical order
                  cnt = cnt + 1
                }
                if (!relMap.contains(oneEdge(3))) {
                  println(cnt)
                  relMap.put(oneEdge(3), cnt)
                  cnt = cnt + 1
                }
                //creating the k-edge substructure

                label = label + oneEdge(0) + "," + oneEdge(2) + "," + oneEdge(4) + "," + relMap.get(oneEdge(1)).toList.head+ "," + relMap.get(oneEdge(3)).toList.head+ ";"
                println(label + "   cnt in relMap " + cnt)


              }
              keyT.set(label)
              valT.set(toBeEmitted.mkString(";"))
              context.write(keyT, valT)
              relMap.clear()


            }
          }
        }
        /*if(!visited.isEmpty && !visE.isEmpty)  {
          var visitediter: Iterator[String] = visited.iterator
          while (visitediter.hasNext) {
            var v = visitediter.next()
            */
        visE.empty
        visited.empty


      }

    }
  }



  class GraphReduce extends Reducer[Text, Text, Text, Text] {

    val dupRemovedInstances: LinkedHashSet[String] = LinkedHashSet() //holds unique instances
    val beamMap: LinkedHashMap[Double, LinkedHashSet[String]] = LinkedHashMap()
    var adjacencyMap: LinkedHashMap[String, LinkedHashSet[String]] = LinkedHashMap()
    var visitedNodes: LinkedHashSet[String] = LinkedHashSet()
    var nonOverlapSubs: LinkedHashSet[String] = LinkedHashSet() // non overlapping

    var noOfVertices: Int = 0
    var noOfEdges: Int = 0
    var noOfInstances: Int = 0
    var num1, num2, num3, dec1: Double = 0
    var mdl: Double = 0

    var edges: LinkedHashSet[String] = LinkedHashSet()
    // What is called in scal

    val res: Text = new Text()
    val k:Text =new Text()
    override def setup(context: Reducer[Text, Text, Text, Text]#Context) {
      try {
              var str: String = null
        val cacheFiles: Array[URI] = context.getCacheFiles

        if (cacheFiles != null && cacheFiles.length > 0) {


          var line: String = "";
          val fs: FileSystem = FileSystem.get(context.getConfiguration)
          val pt: Path = new Path(cacheFiles(0).toString)
          val br = new BufferedReader(new InputStreamReader(fs.open(pt)))

          var str1: String = br.readLine


          while (str1 != null) {
            //keyVal = str.split("[\t]")
            //val vId = keyVal(0)
            edges.add(str1)
            str1 = br.readLine
          }


          val s = edges.groupBy(x => x.split(",")(1))
          val d = edges.groupBy(x => x.split(",")(3))
          adjacencyMap = adjacencyMap ++ s
          adjacencyMap = adjacencyMap ++ d


        }
      }
      catch {
        case e: IOException => println("Unable to Read File")
      }
    }
    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {




        var str1 = ""
        val v = values.iterator()

        while (v.hasNext) {

          var edge = v.next()
          if (edge.toString != "") {
            str1 = str1 + edge + ";;"
          }

        }
        str1 = str1.substring(0, (str1.length - 1))
        //res.set(str)
        var su = key.toString
        var subs1 = su.split(";")



        for (oneSub <- subs1 ) {
          if (oneSub.getClass.toString != "class scala.None$" && !oneSub.equals("")) {
            val subs: Array[String] = oneSub.split(",")
            if (subs.length == 5 && subs(3).forall(Character.isDigit) && subs(4).forall(Character.isDigit))  {

              if (noOfVertices <= Integer.parseInt(subs(3))) {
                noOfVertices = Integer.parseInt(subs(3))
              }
              if (noOfVertices <= Integer.parseInt(subs(4))) {
                noOfVertices = Integer.parseInt(subs(4))
              }
              noOfEdges = noOfEdges + 1;
            }
          }
        }

        var reduces: Array[String] = str1.split(";;")

        for (vv <- reduces) {

          var subVList: Array[String] = vv.split(",")
          //var subVList1: Array[String] = vv.split(",")
          //var l =
          var count: Integer = 0
          //for(subv <- subVList.slice(1,subVList.length) by 2){
          for (i <- 1 to subVList.length - 1 by 2) {
            var sub = subVList(i)
            if (visitedNodes.contains(sub)) {
              break
            }

            count = count + 1

          }
          if (count == 2 * vv.split(";").length) {
            nonOverlapSubs.add(vv)
            //context.write(new Text(vv), new Text(reduces.mkString(";;"))) // all substructure instances and current instance under consideration
            //adding the current edge,substructure  only

          }
        }
        var dupTimeEnd: Long = System.currentTimeMillis()
        val t0 = System.currentTimeMillis()
        //nonOverlapSubs now contains all substructures
        var noOfInstances = nonOverlapSubs.size;
        var num1 = 5 - noOfInstances * noOfVertices + noOfInstances; //INPUT_GRAPH_VERTEX
        var num2 = 6 - noOfInstances * noOfEdges; //INPUT_GRAPH_EDGE
        var num3 = noOfEdges + noOfVertices;
        var dec1 = 5 + 6; //INPUT_GRAPH_EDGE+INP
        var mdl: Double = (num1 + num2 + num3) / dec1;

        if (beamMap.keySet.size < 2) {
          if (beamMap.contains(mdl)) {

            var m1 = beamMap.get(mdl).toList.head // There will be many mdls, it returns like option

            if (m1.isInstanceOf[LinkedHashSet[String]]) {

              m1.addAll(nonOverlapSubs)
              beamMap.put(mdl, m1)
            }
          }

          else {

            beamMap.put(mdl, nonOverlapSubs);
          }
        }
        //var kv = a.zipAll(b,a(0), b(0));
        //var kv1= l.map(i => i._1 -> i._2).toMap;
        else {
          var max: Double = 0.0;
          for (d <- beamMap.keySet) {
            if (max <= d) {
              max = d
            }
          }
          //now I have found my max mdl so check current mdl with max mdl
          if (mdl > max) {
            //remove the max from beamMap
            beamMap.remove(max)
            beamMap.put(mdl, nonOverlapSubs)
          }
        }


        for (d <- beamMap.keySet) {
          var res1 =  beamMap.get(d).toList.head
           k.set(d.toString)
          for (instance:String <- res1) {
            res.set(instance)
            context.write(k,res)
          }
        }


      }

    }



  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    //                Configuration.dumpConfiguration(conf,new PrintWriter(System.out)) // for verfying your conf file
    //            println("Libjars: " + conf.get("tmpjars")); //for making sure your jars have been include
    val job = Job.getInstance(conf,"Graph_Dynamic_Partitioning")

    job.setJarByClass(this.getClass)
    //job.setNumReduceTasks(0)
    job.setMapperClass(classOf[GraphMap])
    job.setCombinerClass (classOf[GraphReduce])
    job.setReducerClass (classOf[GraphReduce])
    job.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text,Text]])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(args(0))) //hdfs://hadoop.fast:54310/user/krithika/small_graph/fin1.txt
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    try {

      // the complete URI(Uniform Resource
      // Identifier) file path in Hdfs
      //job.addCacheFile(new URI("hdfs://localhost:9000/user/krithika/mr/sample_graph.g"))
      job.addCacheFile(new URI("hdfs://graphsmall-m/T10KV20KE_embed_5_vl_10_el_20_mr.g"))
    }
    catch {
      case e: Exception => System.out.println("File Not Added")
        System.exit(1)

    }
    //job.setNumReduceTasks(0)
    System.exit(if(job.waitForCompletion(true))  0 else 1)

    //System.exit(if(job.waitForCompletion(true))  0 else 1)

  }
}
