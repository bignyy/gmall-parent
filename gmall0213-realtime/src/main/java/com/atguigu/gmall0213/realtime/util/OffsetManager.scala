package com.atguigu.gmall0213.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManager {
  def getOffset(topic: String, consumerGroupId: String):Map[TopicPartition, Long]= {
    ///  redis  type? hash   key  ? 主题1：消费者组1  field ?  分区 value ?偏移量
    //利用jedisutil创建jedis对象
    val jedis = RedisUtil.getJedisClient
    //拼接hesh中的key
    val offsetkey = topic + ":" + consumerGroupId
    //获取redis中的offset信息
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetkey)
    jedis.close()
    //判断offset信息，并转化为getKafkaStream创建DStream参数所需类型offsets:Map[TopicPartition,Long]

    if (offsetMap != null && offsetMap.size > 0) {
      //集合隐式转换
      import scala.collection.JavaConversions._
      val offsetList: List[(String, String)] = offsetMap.toList

      val offsetListForkafka: List[(TopicPartition, Long)] = offsetList.map {
        case (partition, offset) =>
          val topicPartition = new TopicPartition(topic, partition.toInt)
          println("加载偏移量：分区：" + partition + "==>" + offset)
          (topicPartition, offset.toLong)
      }
      val offsetMapForkafka: Map[TopicPartition, Long] = offsetListForkafka.toMap

      offsetMapForkafka
    } else {
      null
    }
  }
    def saveOffset(topic:String,groupId:String,offsetRanges:Array[OffsetRange]): Unit ={
      ///  redis  type? hash   key  ? 主题1：消费者组1  field ?  分区 value ?偏移量结束点
      val offsetKey=topic+":"+groupId
      val offsetMap:util.Map[String,String]=new util.HashMap[String,String]() //用来存储多个分区的偏移量
      for(offsetRange <- offsetRanges){
        val partition: String = offsetRange.partition.toString
        val untilOffset: String = offsetRange.untilOffset.toString
        println("写入偏移量：分区："+partition+"==>"+untilOffset)
        offsetMap.put(partition,untilOffset)
      }
      val jedis: Jedis = RedisUtil.getJedisClient
      jedis.hmset(offsetKey,offsetMap)
      jedis.close()

    }



}
