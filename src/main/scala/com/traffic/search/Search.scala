package com.traffic.search

import java.text.SimpleDateFormat
import java.util
import java.util._
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import com.common.Pools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Table, Connection => HbaseConnection}
import org.apache.hadoop.hbase.util.Bytes
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortOrder
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.util.control.Breaks._

/**
  * Created by linux on 17-4-20.
  */
class Search {
  var conn: HbaseConnection = _
  //连接已经建立
  var client: TransportClient = _
  var gettable: Table = _
  //连接已建立
  val redis = initRedis
  val tablename = TableName.valueOf("Result")
  val mapper = new ObjectMapper()
  val TASK = "TASK"
  val CAMERA = "CAMERA"
  val DATASOURCE = "DATASOURCE"
  var num = 0

  //查询一
  def searchElasticHBase(vehicleBrand: String, PlateColor: String, Direction: String, tag: String
                         , paper: String, sun: String, drop: String
                         , secondBelt: String, crash: String, danger: String
                         , starttime: String, endtime: String
                        ): Unit = {
    conn = Search.getHbaseConn
    client = Search.getEsClient
    gettable = conn.getTable(tablename)
    //建立ES索引连接
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    //ES查询Json代码
    val qu = "{\n" +
      "    \"bool\": {\n" +
      "      \"must\": [\n" +
      "        {\n" +
      "          \"term\": {\n" +
      "            \"vehicleBrand.keyword\": {\n" +
      "              \"value\": \"" + vehicleBrand + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"PlateColor.keyword\": {\n" +
      "              \"value\": \"" + PlateColor + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"Direction\": {\n" +
      "              \"value\": \"" + Direction + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"tag.keyword\": {\n" +
      "              \"value\": \"" + tag + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"paper.keyword\": {\n" +
      "              \"value\": \"" + paper + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"sun.keyword\": {\n" +
      "              \"value\": \"" + sun + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"drop.keyword\": {\n" +
      "              \"value\": \"" + drop + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"secondBelt.keyword\": {\n" +
      "              \"value\": \"" + secondBelt + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"crash.keyword\": {\n" +
      "              \"value\": \"" + crash + "\"\n" +
      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
      "          \"term\": {\n" +
      "            \"danger.keyword\": {\n" +
      "              \"value\": \"" + danger + "\"\n" +
      "            }\n" + "          }\n" + "        }\n" + "      ],\n" +
      "      \"filter\": {\n" +
      "        \"range\": {\n" +
      "          \"ResultTime\": {\n" +
      "            \"gte\": \"" + starttime + "\",\n" +
      "            \"lte\": \"" + endtime + "\"\n" +
      "          }\n" + "        }\n" + "      }\n" + "    }\n" +
      "  }"
    //执行查询语句
    val starttimes = System.currentTimeMillis()
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu))
      .setScroll(new TimeValue(60000)).setSize(50).execute().actionGet()
    println("------------------++++------------Search Spend Time:" + (System.currentTimeMillis() - starttimes))
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)
    do {
      val rst = response.getHits.getHits.map(r =>
        (r.getSource.get("resultId").toString, (r.getSource.get("taskId").toString, r.getSource.get("cameraId").toString,
          r.getSource.get("dataSourceId").toString)))

      val gets = rst.map(r => new Get(r._1.getBytes)).toList
      val ids = rst.map(r => r._2)

      val starttime = System.currentTimeMillis()
      val resultHBase = searchHBase(gets)
      val resultRdeis = searchRedis(ids)
      joinResult(resultHBase, resultRdeis)
      println("------------------------------Search Spend Time:" + (System.currentTimeMillis() - starttime))

      print("Whether to return to the next batch Ｙ／Ｎ:")
      var s: Scanner = new Scanner(System.in)
      val flag = s.nextLine()
      if (flag.equalsIgnoreCase("N")) {
        return
      }
      response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
    } while (response.getHits.getHits.length != 0)
    Search.returnEsConn(client)
    Search.returnHbaseConn(conn)
  }

  def joinResult(resultHBase: util.LinkedList[util.HashMap[String, String]], resultRdeis: util.LinkedList[(util.HashMap[String, String], util.HashMap[String, String], util.HashMap[String, String])]) = {
    val results = new util.LinkedList[util.HashMap[String, String]]()
    if (resultHBase.size() == resultRdeis.size()) {
      var flag = resultHBase.size()

      while (flag > 0) {
        val tmp = new HashMap[String, String]()

        val hr = resultHBase.get(flag)
        val rr = resultRdeis.get(flag)

        if (hr != null) {
          tmp.putAll(hr)
        }
        if (rr._1 != null) {
          tmp.putAll(rr._1)
        }
        if (rr._2 != null) {
          tmp.putAll(rr._2)
        }
        if (rr._3 != null) {
          tmp.putAll(rr._3)
        }
        results.add(tmp)
        flag = flag - 1
      }
    }
    println(results)
    results
  }


  def searchRedis(ids: Array[(String, String, String)]) = {
    val result = new util.LinkedList[Tuple3[HashMap[String, String], HashMap[String, String], HashMap[String, String]]]()
    ids.foreach(i => {
      if (redis.hexists(TASK, i._1)) {
        val task = mapper.readValue(redis.hget(TASK, i._1), new HashMap[String, String].getClass)
      }
      if (redis.hexists(CAMERA, i._2)) {
        val task = mapper.readValue(redis.hget(CAMERA, i._2), new HashMap[String, String].getClass)
      }
      if (redis.hexists(DATASOURCE, i._3)) {
        val task = mapper.readValue(redis.hget(DATASOURCE, i._3), new HashMap[String, String].getClass)
      }
    })
    result
  }

  //查询结果
  def joinElasticHBase(resHBase: Map[String, String], resRedis: Map[String, String]): Map[String, String] = {
    val result = new HashMap[String, String]
    if (resHBase != null) {
      result.putAll(resHBase)
    }
    if (resRedis != null) {
      result.putAll(resRedis)
    }
    println("----" + result)
    result
  }

  //查询结果
  def joinRedis(resRedistask: Map[String, String], resRediscamera: Map[String, String], resRedisdataSource: Map[String, String]): Map[String, String] = {
    val result = new HashMap[String, String]
    if (resRedistask != null) {
      result.putAll(resRedistask)
    }
    if (resRediscamera != null) {
      result.putAll(resRediscamera)
    }
    if (resRedisdataSource != null) {
      result.putAll(resRedisdataSource)
    }
    println("----" + result)
    result
  }

  //执行Redis查询
  def searchRedis(table: String, id: String): HashMap[String, String] = {
    var map: HashMap[String, String] = null
    if (redis.hexists(table, id)) {
      val result = redis.hget(table, id)
      map = mapper.readValue(result, new HashMap[String, String].getClass)
    }
    map
  }

  //建立Redis连接
  def initRedis: JedisCluster = {
    val jedisClusterNodes = new HashSet[HostAndPort]()
    //在添加集群节点的时候只需要添加一个，其余同一集群的J节点会被自动加入
    jedisClusterNodes.add(new HostAndPort("172.20.31.4", 6380))
    val jc: JedisCluster = new JedisCluster(jedisClusterNodes)
    jc
  }

  //HBase查询代码
  def searchHBase(gets: List[Get]): util.LinkedList[HashMap[String, String]] = {
    val getsJ = JavaConversions.seqAsJavaList(gets)
    val resultsFinal = new util.LinkedList[util.HashMap[String, String]]()
    val result = gettable.get(getsJ)
    result.foreach(result => {
      val resultHbase = new util.HashMap[String, String]()
      val License = Bytes.toString(result.getValue("Result".getBytes, "License".getBytes))
      val PlateType = Bytes.toString(result.getValue("Result".getBytes, "PlateType".getBytes))
      val PlateColor = Bytes.toString(result.getValue("Result".getBytes, "PlateColor".getBytes))
      val Confidence = Bytes.toString(result.getValue("Result".getBytes, "Confidence".getBytes))
      val LicenseAttribution = Bytes.toString(result.getValue("Result".getBytes, "LicenseAttribution".getBytes))
      val ImageURL = Bytes.toString(result.getValue("Result".getBytes, "ImageURL".getBytes))
      val CarColor = Bytes.toString(result.getValue("Result".getBytes, "CarColor".getBytes))
      val ResultTime = Bytes.toString(result.getValue("Result".getBytes, "ResultTime".getBytes))
      val Direction = Bytes.toString(result.getValue("Result".getBytes, "Direction".getBytes))
      val frame_index = Bytes.toString(result.getValue("Result".getBytes, "frame_index".getBytes))
      val vehicleKind = Bytes.toString(result.getValue("Result".getBytes, "vehicleKind".getBytes))
      val vehicleBrand = Bytes.toString(result.getValue("Result".getBytes, "vehicleBrand".getBytes))
      val vehicleStyle = Bytes.toString(result.getValue("Result".getBytes, "vehicleStyle".getBytes))
      val LocationLeft = Bytes.toString(result.getValue("Result".getBytes, "LocationLeft".getBytes))
      if (License != null) {
        resultHbase.put("HP", License)
        println(result)
      }
      resultsFinal.add(resultHbase)
    })
    resultsFinal
  }

  //查询三
  def searchCarnumber(starttime: String, endtime: String): Unit = {
    client = Search.getEsClient
    //建立ES索引连接
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    //ES查询Json代码
    val qu = "{\n" +
      "    \"range\": {\n" +
      "      \"ResultTime\": {\n" +
      "        \"gte\": \"" + starttime + "\",\n" +
      "        \"lte\": \"" + endtime + "\"\n" +
      "      }\n" +
      "    }\n" +
      "  }"
    //执行查询语句
    val starttimes = System.currentTimeMillis()
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu)).execute().actionGet()
    println("------------------++++------------Search Spend Time:" + (System.currentTimeMillis() - starttimes))
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)

    val start = System.nanoTime()
    response.getHits.getTotalHits.toString
    println("------------------------------Search Spend Time:" + (System.nanoTime() - start) / 1000)
    Search.returnEsConn(client)
  }

  //查询四
  def searchLicense(starttime: String, province: String, regexnumber: String): Unit = {
    val simplehms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now = simplehms.format(new Date(System.currentTimeMillis()))
    var num = 0
    client = Search.getEsClient
    //建立ES索引连接
    val request: SearchRequestBuilder = client.prepareSearch().setIndices("vehicle").setTypes("result")
    //ES查询Json代码
    val qu = "{\n" +
      "    \"bool\": {\n" +
      "      \"must\": [\n" +
      "        {\n" +
      "          \"regexp\": {\n" +
      "            \"Plate.keyword\": {\n" +
      "              \"value\": \"" + province + ".{5}" + regexnumber + "\"\n" +
      "            }\n" +
      "          }\n" +
      "        },\n" +
      "        {\n" +
      "          \"range\": {\n" +
      "            \"ResultTime\": {\n" +
      "              \"gte\": \"" + starttime + "\",\n" +
      "              \"lte\": \"" + now + "\"\n" +
      "            }\n" +
      "          }\n" +
      "        }\n" +
      "      ]\n" +
      "    }\n" +
      "  }"
    //执行查询语句
    val starttimes = System.currentTimeMillis()
    var response: SearchResponse = request.setQuery(QueryBuilders.wrapperQuery(qu)).setSize(50)
      .setScroll(new TimeValue(60000)).execute().actionGet()
    println("------------------++++------------Search Spend Time:" + (System.currentTimeMillis() - starttimes))
    println("-----Search hit total data:" + response.getHits.getTotalHits.toString)
    do {
      val starttime = System.currentTimeMillis()
      for (rs <- response.getHits.getHits) {
        //异步查询
        val task = searchRedis(TASK, rs.getSource.get("taskId").toString)
        val camera = searchRedis(CAMERA, rs.getSource.get("cameraId").toString)
        val dataSource = searchRedis(DATASOURCE, rs.getSource.get("dataSourceId").toString)
        println(task)
      }
      println("------------------------------Search Spend Time:" + (System.currentTimeMillis() - starttime))
      print("Whether to return to the next batch Ｙ／Ｎ:")
      var s: Scanner = new Scanner(System.in)
      val flag = s.nextLine()
      if (flag.equalsIgnoreCase("N")) {
        return
      }
      response = client.prepareSearchScroll(response.getScrollId).setScroll(new TimeValue(60000)).execute().actionGet()
    } while (response.getHits.getHits.length != 0)
    Search.returnEsConn(client)
  }
}

object Search extends Pools {
  //创建线程池
  val pool: ExecutorService = Executors.newWorkStealingPool(50)

  def main(args: Array[String]): Unit = {
    val sbe = new Search
    while (true) {
      System.out.println("Please select a number to choose the task")
      System.out.println("1. Examples of usage(Input parameters):VehicleBrand PlateColor Direction  tag paper sun drop secondBelt crash danger startTime endTime")
      System.out.println("2. Examples of usage(Input parameters):startTime endTime")
      System.out.println("3. Examples of usage(Input parameters):startTime province regexnumber")
      System.out.println("other. stop the task")
      var s: Scanner = new Scanner(System.in)
      System.out.print("Enter your choice : ")
      var line = s.nextLine()
      line match {
        case "1" =>
          System.out.println("Examples of usage(Parameters are separated by |): " + "\"马自达\"|" + "\"黄\"|" + "\"1\"|" + "\"true\"|" + "\"false\"|" + "\"false\"|" + "\"true\"|" + "\"true\"|" + "\"true\"|" + "\"false\"|" + "\"2017-04-10 15:37:02\"|" + "\"2017-04-20 15:37:02\"")
          System.out.print("Input parameters : ")
          var res = s.nextLine()
          var flag = true
          while (flag) {
            if (res.split("\\|").length != 12) {
              println("Invalid parameter, please re-enter")
              print("Input parameters : ")
              res = s.nextLine()
            } else {
              flag = false
            }
          }
          sbe.searchElasticHBase(res.split("\\|")(0).replace("\"", ""), res.split("\\|")(1).replace("\"", ""), res.split("\\|")(2).replace("\"", ""), res.split("\\|")(3).replace("\"", ""),
            res.split("\\|")(4).replace("\"", ""), res.split("\\|")(5).replace("\"", ""), res.split("\\|")(6).replace("\"", ""), res.split("\\|")(7).replace("\"", ""), res.split("\\|")(8).replace("\"", ""),
            res.split("\\|")(9).replace("\"", ""), res.split("\\|")(10).replace("\"", ""), res.split("\\|")(11).replace("\"", ""))
        case "2" =>
          System.out.println("Examples of usage(Parameters are separated by |): " + "\"2017-04-20 15:37:02\"|" + "\"2017-04-25 15:37:02\"")
          System.out.print("Input parameters : ")
          var res = s.nextLine()
          var flag = true
          while (flag) {
            if (res.split("\\|").length != 2) {
              println("Invalid parameter, please re-enter")
              print("Input parameters : ")
              res = s.nextLine()
            } else {
              flag = false
            }
          }
          sbe.searchCarnumber(res.split("\\|")(0).replace("\"", ""), res.split("\\|")(1).replace("\"", ""))
        case "3" =>
          System.out.println("Examples of usage(Parameters are separated by |): " + "\"2017-04-20 15:37:02\"|" + "\"粤\"|" + "\"2\"")
          System.out.print("Input parameters : ")
          var res = s.nextLine()
          var flag = true
          while (flag) {
            if (res.split("\\|").length != 3) {
              println("Invalid parameter, please re-enter")
              print("Input parameters : ")
              res = s.nextLine()
            } else {
              flag = false
            }
          }
          sbe.searchLicense(res.split("\\|")(0).replace("\"", ""), res.split("\\|")(1).replace("\"", ""), res.split("\\|")(2).replace("\"", ""))
        case other => break
      }
      println("\n\n")
    }

  }
}