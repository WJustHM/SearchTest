//////package com.traffic.search
//////
//////import java.util.HashSet
//////
//////import com.common.Pools
//////import org.apache.hadoop.hbase.client.{Connection => HbaseConnection}
//////import redis.clients.jedis.{HostAndPort, JedisCluster}
//////
///////**
//////  * Created by linux on 17-5-5.
//////  */
//////object Connection extends Pools {
//////  val conn: HbaseConnection = getHbaseConn
//////  //连接已经建立
//////  val client = getEsClient
//////  //连接已建立
//////  val redis = initRedis
//////
//////  //建立Redis连接
//////  def initRedis: JedisCluster = {
//////    val jedisClusterNodes = new HashSet[HostAndPort]()
//////    //在添加集群节点的时候只需要添加一个，其余同一集群的J节点会被自动加入
//////    jedisClusterNodes.add(new HostAndPort("172.20.31.4", 6380))
//////    val jc: JedisCluster = new JedisCluster(jedisClusterNodes)
//////    jc
//////  }
//////
//////  def main(args: Array[String]): Unit = {
//////    while (true) {
//////    }
//////  }
//////}
//////
//////
//
//package com.example
//
//import java.util
//import java.util.concurrent.{Callable, ExecutorService, Executors, Future}
//import java.util.{HashMap, HashSet}
//
//import com.common.Pools
//import org.apache.hadoop.hbase.TableName
//import org.apache.hadoop.hbase.client.Get
//import org.apache.hadoop.hbase.util.Bytes
//import org.codehaus.jackson.map.ObjectMapper
//import org.elasticsearch.action.search.SearchResponse
//import org.elasticsearch.common.unit.TimeValue
//import org.elasticsearch.index.query.QueryBuilders
//import org.elasticsearch.search.sort.SortOrder
//import redis.clients.jedis.{HostAndPort, JedisCluster}
//
//import scala.collection.JavaConversions
//
///**
//  * Created by xuefei_wang on 17-5-9.
//  */
//
//class MainSearch extends Pools{
//  val hbaseHandler  = getHbaseConn.getTable(TableName.valueOf("Result"))
//  val esclient = getEsClient
//  val esHandler = esclient.prepareSearch().setIndices("vehicle").setTypes("result")
//  val redis = initRedis
//  val batchSize = 50
//  val mapper = new ObjectMapper()
//  val TASK = "TASK"
//  val CAMERA = "CAMERA"
//  val DATASOURCE = "DATASOURCE"
//
//
//  def initRedis: JedisCluster = {
//    val jedisClusterNodes = new HashSet[HostAndPort]()
//    jedisClusterNodes.add(new HostAndPort("172.20.31.4", 6380))
//    val jc: JedisCluster = new JedisCluster(jedisClusterNodes)
//    jc
//  }
//
//
//  def searchHBase(gets: List[Get]): util.LinkedList[HashMap[String, String]] = {
//    val getsJ  =JavaConversions.seqAsJavaList(gets)
//    val resultsFinal = new util.LinkedList[util.HashMap[String, String]]()
//    val results = hbaseHandler.get(getsJ)
//    results.foreach( result =>{
//      val resultHbase = new util.HashMap[String, String]()
//      val License = Bytes.toString(result.getValue("Result".getBytes, "License".getBytes))
//      val PlateType = Bytes.toString(result.getValue("Result".getBytes, "PlateType".getBytes))
//      val PlateColor = Bytes.toString(result.getValue("Result".getBytes, "PlateColor".getBytes))
//      val Confidence = Bytes.toString(result.getValue("Result".getBytes, "Confidence".getBytes))
//      val LicenseAttribution = Bytes.toString(result.getValue("Result".getBytes, "LicenseAttribution".getBytes))
//      val ImageURL = Bytes.toString(result.getValue("Result".getBytes, "ImageURL".getBytes))
//      val CarColor = Bytes.toString(result.getValue("Result".getBytes, "CarColor".getBytes))
//      val ResultTime = Bytes.toString(result.getValue("Result".getBytes, "ResultTime".getBytes))
//      val Direction = Bytes.toString(result.getValue("Result".getBytes, "Direction".getBytes))
//      val frame_index = Bytes.toString(result.getValue("Result".getBytes, "frame_index".getBytes))
//      val vehicleKind = Bytes.toString(result.getValue("Result".getBytes, "vehicleKind".getBytes))
//      val vehicleBrand = Bytes.toString(result.getValue("Result".getBytes, "vehicleBrand".getBytes))
//      val vehicleStyle = Bytes.toString(result.getValue("Result".getBytes, "vehicleStyle".getBytes))
//      val LocationLeft = Bytes.toString(result.getValue("Result".getBytes, "LocationLeft".getBytes))
//      resultHbase.put("HP", License)
//      resultsFinal.add(resultHbase)
//    }
//    )
//    resultsFinal
//  }
//
//
//
//  def searchRedis(ids: Array[(String, String, String)]) = {
//    val result = new util.LinkedList[Tuple3[HashMap[String, String],HashMap[String, String],HashMap[String, String]]]()
//    ids.foreach( i => {
//      val task = mapper.readValue(redis.hget(TASK,i._1), new HashMap[String, String].getClass)
//      val camera =  mapper.readValue(redis.hget(CAMERA,i._2), new HashMap[String, String].getClass)
//      val datasource = mapper.readValue(redis.hget(DATASOURCE,i._3), new HashMap[String, String].getClass)
//      result.add((task,camera,datasource))
//    })
//    result
//  }
//
//  def joinResult(resultHBase: util.LinkedList[util.HashMap[String, String]], resultRdeis: util.LinkedList[(util.HashMap[String, String], util.HashMap[String, String], util.HashMap[String, String])]) = {
//
//    val results = new util.LinkedList[util.HashMap[String, String]]()
//    if ( resultHBase.size() == resultRdeis.size()){
//      var flag = resultHBase.size()
//
//      while (flag > 0){
//        val tmp = new HashMap[String, String]()
//
//        val hr = resultHBase.get(flag)
//        val rr = resultRdeis.get(flag)
//
//        if (hr != null){
//          tmp.putAll(hr)
//        }
//        if (rr._1 != null){
//          tmp.putAll(rr._1)
//        }
//        if (rr._2 != null){
//          tmp.putAll(rr._2)
//        }
//        if (rr._3 != null){
//          tmp.putAll(rr._3)
//        }
//        results.add(tmp)
//        flag = flag -1
//      }
//
//    }
//    results
//  }
//
//  def searchOne(vehicleBrand: String, PlateColor: String, Direction: String, tag: String
//                , paper: String, sun: String, drop: String
//                , secondBelt: String, crash: String, danger: String
//                , starttime: String, endtime: String
//               ): Unit = {
//    val search  = "{\n" +
//      "    \"bool\": {\n" +
//      "      \"must\": [\n" +
//      "        {\n" +
//      "          \"term\": {\n" +
//      "            \"vehicleBrand.keyword\": {\n" +
//      "              \"value\": \"" + vehicleBrand + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"PlateColor.keyword\": {\n" +
//      "              \"value\": \"" + PlateColor + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"Direction\": {\n" +
//      "              \"value\": \"" + Direction + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"tag.keyword\": {\n" +
//      "              \"value\": \"" + tag + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"paper.keyword\": {\n" +
//      "              \"value\": \"" + paper + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"sun.keyword\": {\n" +
//      "              \"value\": \"" + sun + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"drop.keyword\": {\n" +
//      "              \"value\": \"" + drop + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"secondBelt.keyword\": {\n" +
//      "              \"value\": \"" + secondBelt + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"crash.keyword\": {\n" +
//      "              \"value\": \"" + crash + "\"\n" +
//      "            }\n" + "          }\n" + "        },\n" + "        {\n" +
//      "          \"term\": {\n" +
//      "            \"danger.keyword\": {\n" +
//      "              \"value\": \"" + danger + "\"\n" +
//      "            }\n" + "          }\n" + "        }\n" + "      ],\n" +
//      "      \"filter\": {\n" +
//      "        \"range\": {\n" +
//      "          \"ResultTime\": {\n" +
//      "            \"gte\": \"" + starttime + "\",\n" +
//      "            \"lte\": \"" + endtime + "\"\n" +
//      "          }\n" + "        }\n" + "      }\n" + "    }\n" +
//      "  }"
//
//    var response: SearchResponse = esHandler.setQuery(QueryBuilders.wrapperQuery(search))
//      .addSort("ResultTime", SortOrder.ASC).setScroll(new TimeValue(60000)).setSize(batchSize).execute().actionGet()
//
//    val fuList = new util.LinkedList[Future[util.Map[String, String]]]()
//    do {
//
//      val rst =  response.getHits.getHits.map( r =>
//        (r.getSource.get("resultId").toString , (r.getSource.get("taskId").toString, r.getSource.get("cameraId").toString ,
//          r.getSource.get("dataSourceId").toString)))
//
//      val gets = rst.map( r => new Get(r._1.getBytes)).toList
//      val ids = rst.map(r => r._2)
//
//      val res = MainSearch.pool.submit(new Callable[util.Map[String, String]] {
//
//        override def call(): util.LinkedList[util.HashMap[String, String]] = {
//          val resultHBase = searchHBase(gets)
//          val resultRdeis = searchRedis(ids)
//          joinResult(resultHBase, resultRdeis)
//          resultHBase
//        }})
//      println(res)
//
//      response = esclient.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
//    } while (response.getHits.getHits.length != 0)
//
//  }
//
//
//  def searchThree(starttime: String, endtime: String): Unit = {
//
//
//  }
//
//  def searchFour(starttime: String, province: String, regexnumber: String): Unit = {
//
//
//  }
//
//  def start(): Unit ={
//
//    var flag = true
//
//    while (flag){
//
//      println("请选择查询种类：　\n　输入　1　表示查询一  \n 输入　2　表示查询二  \n  输入　3　表示查询三   \n 输入　4　表示查询四  \n 输入　0　表示退出  \n")
//      val op = System.in.read().toInt
//      if (op == 1){
//
//      }
//
//      if (op == 2){
//
//      }
//
//      if (op == 3){
//
//      }
//
//      if (op == 4){
//
//      }
//
//      if (op == 0){
//        System.exit(0)
//      }
//
//    }
//  }
//
//}
//
//object MainSearch {
//
//  val pool: ExecutorService = Executors.newWorkStealingPool(50)
//
//  def main(args: Array[String]): Unit = {
//
//  }
//
//}
//
