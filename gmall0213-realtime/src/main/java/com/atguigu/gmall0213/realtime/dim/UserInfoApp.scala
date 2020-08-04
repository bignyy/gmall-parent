package com.atguigu.gmall0213.realtime.dim

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.bean.dim.UserInfo
import com.atguigu.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserInfoApp {
  def main(args: Array[String]): Unit = {

    //1 从ods层(kafka) 获得对应维表数据  //2 偏移量后置 幂等
    //2 数据转换 case class
    //3 保存到hbase(phoenix)
    val sc: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_user_info")
    val ssc = new StreamingContext(sc,Seconds(5))
    val groupId="dim_user_info_group"
    val topic="ODS_USER_INFO"
    //1   从redis中读取偏移量   （启动执行一次）
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMapForKafka!=null&&offsetMapForKafka.size>0){

       recordInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMapForKafka,groupId)
    }else{
       recordInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd =>
      val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val userinfoDstream = inputGetOffsetDstream.map {
      record =>
        val json: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(json, classOf[UserInfo])
        //把生日转成年龄
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val date: util.Date= formattor.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val  betweenMs= curTs-date.getTime
        val age=betweenMs/1000L/60L/60L/24L/365L
        if(age<20){
          userInfo.age_group="20岁及以下"
        }else if(age>30){
          userInfo.age_group="30岁以上"
        }else{
          userInfo.age_group="21岁到30岁"
        }
        if(userInfo.gender=="M"){
          userInfo.gender_name="男"
        }else{
          userInfo.gender_name="女"
        }
        userInfo
    }
    //4  1 写入phoenix 2 提交偏移量
    userinfoDstream.foreachRDD{
      rdd=>
        import org.apache.phoenix.spark._
        rdd.saveToPhoenix("GMALL0213_User_INFO",
          Seq("ID", "USER_LEVEL",  "BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
        OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }


  }
}
