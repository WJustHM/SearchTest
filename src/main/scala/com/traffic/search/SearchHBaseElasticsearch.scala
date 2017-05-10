package com.traffic.search

import com.common.Pools
import org.apache.hadoop.hbase.client.{BufferedMutator, Connection => HbaseConnection}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentFactory

import scala.util.Random

/**
  * Created by linux on 17-4-20.
  */
class SearchHBaseElasticsearch extends Pools {
  val clientElastic: TransportClient = getEsClient
  val connHBase: HbaseConnection = getHbaseConn
  val tablename = TableName.valueOf("Result")
  val bufftable: BufferedMutator = connHBase.getBufferedMutator(tablename)

  val pla = Array("宝马", "奔驰", "法拉利", "保时捷", "兰博基尼", "林肯", "布加迪", "宾利", "阿斯顿马丁", "帕加尼")
  val month = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
  val hour = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
  val color = Array("蓝", "红", "黄", "绿")
  val ran = new Random()
  var rowkey = 0;


  def clienttestxpack(): Unit = {
    val se = clientElastic.prepareGet(".kibana", "index-pattern", ".kibana").get()
    println(se.getSource)
  }


  def createHBaseTbale(connHBase: HbaseConnection): Unit = {
    val admin = connHBase.getAdmin
    if (admin.tableExists(tablename)) {
      println("Table exists!")
    } else {
      val tableDesc = new HTableDescriptor(tablename)
      tableDesc.addFamily(new HColumnDescriptor("Result".getBytes))
      admin.createTable(tableDesc)
      println("Create table success!")
    }
  }

  def putDataHBaseElsaticserach(): Unit = {
    val bulkrequest = clientElastic.prepareBulk()
    val bulkProcessor = BulkES(clientElastic)
    //      bulkProcessor.add(new IndexRequest("twitter", "tweet").source(XContentFactory.jsonBuilder().startObject()
    //        .field("ResultTime", time)
    //        .field("PlateColor", platecolor)
    //        .field("Direction", 0)
    //        .field("VehicleBrand", plate)
    //        .field("tag", 1)
    //        .field("drop", 1)
    //        .field("secondBelt", 1)
    //        .field("call", 0)
    //        .field("crash", 0)
    //        .field("danger", 0)
    //        .field("paper", 0)
    //        .field("sun", 0)
    //        .field("resultRowkey", rowkey)
    //        .field("taskId", "007")
    //        .field("cameraId", "007")
    //        .field("datasourceId", "007")
    //        .endObject().string));


    // Elastic
    bulkrequest.add(clientElastic.prepareIndex("wh", "wh").setSource(
      XContentFactory.jsonBuilder().startObject()
        .field("remote_addr", "59.175.137.251")
        .field("time_local", "2016-02-23 11:47；００")
        .field("server_name", 0)
        .field("request", "ew")
        .field("request_length", 1)
        .endObject().string))

  }

  def BulkES(client: Client): BulkProcessor = {
    val bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener {
      override def beforeBulk(l: Long, bulkRequest: BulkRequest): Unit = {
        println("请求数量" + bulkRequest.numberOfActions())
      }

      override def afterBulk(l: Long, bulkRequest: BulkRequest, bulkResponse: BulkResponse): Unit = {
        if (bulkResponse.hasFailures) {
          println(bulkResponse.buildFailureMessage())
        }
      }

      override def afterBulk(l: Long, bulkRequest: BulkRequest, throwable: Throwable): Unit = {
        println("失败请求---------" + throwable.printStackTrace())
      }
    }).setBulkActions(100)
      .setFlushInterval(TimeValue.timeValueSeconds(5))
      .setConcurrentRequests(1)
      .setBackoffPolicy(
        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
      .build()
    bulkProcessor
  }
}

object SearchHBaseElasticsearch {
  def main(args: Array[String]): Unit = {
    val sbe = new SearchHBaseElasticsearch
    sbe.putDataHBaseElsaticserach
    //    sbe.clienttestxpack
  }
}
