import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable._
import Array._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._







object ying_cheng_task2 {
  def find_cadidates(iter: Iterator[List[String]], s: Int): Iterator[List[String]] = {
    var part = iter.toList
    var maxLen: Int = 0
    var filter_set = Set[String]() //string in the hashset
    //    println(filter_set.getClass.getSimpleName)

    for (i <- part) {
      maxLen = math.max(maxLen, i.length)


      //      print(i.length.getClass.getSimpleName)
      for (num <- i) {
        filter_set.add(num)
      }
    }
    //  println(filter_set)
    //  println("maxlen is ", maxLen)


    var all_frequent_set = Set[List[String]]()
    all_frequent_set.add(List[String]())
    //    var s = (support / 2)
    //    println(s)
    for (i <- range(1, maxLen + 1)) {
      //    println("the size",i)
      //    for (i<- range(1, 2))  {
      var ci_count = Map[List[String], Int]()
      for (transaction <- part) {
        var transaction_removed = Set[String]() //set

        for (each_transaction <- transaction) {
          if (filter_set.contains(each_transaction)) {
            transaction_removed.add(each_transaction)
          }
        }
        var transaction_removed_list = transaction_removed.toList
        //        println(transaction_removed_list)
        var temp_ci_transaction = transaction_removed_list.sorted.combinations(i)
        //        temp_ci_transaction.foreach(println)
        for (temp_ci <- temp_ci_transaction) {
          //                  print(temp_ci)
          var sub_temp_ci_transaction = temp_ci.sorted.combinations(i - 1)
          var flag = true
          //        var cheng = true
          //        for (sub_temp_ci <- sub_temp_ci_transaction if cheng) {
          for (sub_temp_ci <- sub_temp_ci_transaction) {

            if (!all_frequent_set.contains(sub_temp_ci)) {
              flag = false
              //            cheng = false
            }
          }
          if (flag) {
            if (ci_count.contains(temp_ci)) {
              //                print(ci_count(temp_ci))
              ci_count(temp_ci) += 1
            }
            else {
              ci_count += (temp_ci -> 1)
            }
          }


        }
      }
      //    println(ci_count)
      //      print(ci_count)
      filter_set = Set[String]()
      for (everyone <- ci_count.keySet) {
        if (ci_count(everyone) >= s) {
          all_frequent_set.add(everyone)
          for (num <- everyone) {
            filter_set.add(num)
          }
        }
      }
    }
    return all_frequent_set.toList.iterator

  }


  def create_key(basket: List[String], candidates_list: List[List[String]]): List[List[String]] = {
    var list_key = new ArrayBuffer[List[String]]
    //    println(list_key.getClass.getSimpleName)
    for (candidates <- candidates_list) {
      var flag = true
      //        println("candidate: "+candidates)
      //        println("basket: "+basket)
      for (single_item <- candidates) {
        if (!basket.contains(single_item)) {
          flag = false
        }
      }
      //        println(flag)
      if (flag) {
        //          println(candidates)
        list_key += candidates
      }
    }
    //      var list_key_list = list_key_set.toList
    //      for(i<- list_key_set){
    //        println(i)
    //        println("sadfasgasd")
    //      }
    //      println(list_key.toList)
    return list_key.toList


  }

  def main()(args: Array[String]) {


    def add(a: Int, b: Int): Int = {
      var sum: Int = 0
      sum = a + b
      return sum
    }


    val conf = new SparkConf().setAppName("Task1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val start = System.currentTimeMillis()

//    val File_path = "/Users/irischeng/INF553/Assignment/hw2/task2_dataset.csv"
    val File_path = args(2)

    //    val outputFile_path = args(1)
    val File = sc.textFile(File_path)
    //    println(File.collect()(0))
    //    var n= File.map(s => s.split(",")).map(s=>(s(0),s(1)))
    //    println(n.collect()(1))
    var s=args(1)
    var support = s.toInt

    var threshold =args(0)
    var k = threshold.toInt




    val header = File.first()
    //    println(header)
    val File_rdd = File.filter(line => line != header).map(s => s.split(",")).map(s => (s(0), Set("'" + s(1) + "'")))
    //      println(File_rdd.collect()(0))
    val basket_u_b = File_rdd.reduceByKey(_ ++ _).filter(s=>s._2.size>k).map(s => s._2.toList).repartition(2) //rdd
    //    println(basket_u_b.getNumPartitions)
    val basket_list_u_b = basket_u_b.repartition(1).collect() //list[list]
    var candidates_u_b = basket_u_b.mapPartitions(x => find_cadidates(x, 25))
    var candidates_u_b_list = candidates_u_b.map(x => x).collect().toSet.toList

    var real_frequent_u_b = basket_u_b.flatMap(basket => create_key(basket, candidates_u_b_list)).map(s => (s, 1)).
      reduceByKey(add).filter(s => s._2 >= support).map(s => s._1).collect().toList

    var output_u_b_can = Map[Int, ArrayBuffer[String]]()
    for (i <- candidates_u_b_list) {
      var str = i.toString().replace("List(", "(")
      //        println(str)
      if (output_u_b_can.contains(i.length)) {
        //                print(ci_count(temp_ci))
        output_u_b_can(i.length) += str
      }
      if (!output_u_b_can.contains(i.length)) {
        output_u_b_can.put(i.length, ArrayBuffer())
        output_u_b_can(i.length) += str
      }
    }

    //    println(output_u_b_can(1).getClass.getSimpleName)


    var output_u_b_fre = Map[Int, ArrayBuffer[String]]()
    for (i <- real_frequent_u_b) {
      var str = i.toString().replace("List(", "(")
      if (output_u_b_fre.contains(i.length)) {
        output_u_b_fre(i.length) += str
      }
      if (!output_u_b_fre.contains(i.length)) {
        output_u_b_fre.put(i.length, ArrayBuffer())
        output_u_b_fre(i.length) += str
      }
    }


    //    println(output_u_b_can(1).last)

    var max_key_can = output_u_b_can.keySet.toList.max
    var max_key_fre = output_u_b_fre.keySet.toList.max
    //    println(max_key_can)
    //write the output
    val writer1 = new PrintWriter(args(3))
    //    writer1.write("city, stars")
    writer1.write("Candidates:\n")
    for (i <- range(1, max_key_can + 1)) {
      var temp_list_can = output_u_b_can(i).sorted
      //      println(i)
      for (j <- temp_list_can) {
        //        println(j)
        writer1.write(j)
        if (j != temp_list_can.last) {
          writer1.write(",")
        }

      }
      writer1.write("\n")
      writer1.write("\n")
    }
    writer1.write("Frequent Itemsets:\n")
    for (i <- range(1, max_key_fre + 1)) {
      var temp_list_fre = output_u_b_fre(i).sorted
      for (j <- temp_list_fre) {
        writer1.write(j)
        if (j != temp_list_fre.last) {
          writer1.write(",")
        }

      }
      writer1.write("\n")
      writer1.write("\n")
    }
    writer1.close()

    val end = System.currentTimeMillis()
    println("Duration:"+(end - start)/1000.0)





  }

}



