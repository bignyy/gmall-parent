package com.atguigu.gmall0213.realtime.dws

import java.{lang, util}
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0213.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0213.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * 订单，订单明细，用2个流从kafka中读DWD，1开窗 2join 3去重（存Redis-set类型）
  * 4.存Redis成功的添加到合并后对象的集合  5，集合.toiterator返回
  * 6.partition->rdd->toDF->write->clickhouse
  *
  */
object OrderWideApp {

  def main(args: Array[String]): Unit = {
    //双流  订单主表  订单从表    偏移量 双份
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dws_order_wide_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoGroupId = "dws_order_info_group"
    val orderInfoTopic = "DWD_ORDER_INFO"
    val orderDetailGroupId = "dws_order_detail_group"
    val orderDetailTopic = "DWD_ORDER_DETAIL"

    //1   从redis中读取偏移量   （启动执行一次）
    val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(orderDetailTopic, orderDetailGroupId)

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMapForKafka, orderInfoGroupId)
    } else {
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }


    var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMapForKafka, orderDetailGroupId)
    } else {
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }


    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var orderInfoOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    // 1 提取数据 2 分topic
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      orderInfo
    }

    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }

    //
    //    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    //    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
    //
    //    val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)


    // 方案一：  1开窗口    2 join 3  去重
    //window
    val orderInfoWindowDstream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(15), Seconds(5)) //窗口大小  滑动步长
    val orderDetailWindowDstream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(15), Seconds(5)) //窗口大小  滑动步长

    // join
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWindowDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailWindowDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

    val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream, 4)

    // 去重
    //  数据统一保存到
    // redis ?  type? set   api? sadd   key ? order_join:[orderId]        value ? orderDetailId (skuId也可）  expire : 60*10
    // sadd 返回如果0  过滤掉
    val orderWideDstream: DStream[OrderWide] = joinedDstream.mapPartitions { tupleItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
      for ((orderId, (orderInfo, orderDetail)) <- tupleItr) {
        val key = "order_join:" + orderId
        val ifNotExisted: lang.Long = jedis.sadd(key, orderDetail.id.toString)
        jedis.expire(key, 600)
        //合并宽表
        if (ifNotExisted == 1L) {
          orderWideList.append(new OrderWide(orderInfo, orderDetail))
        }
      }
      jedis.close()
      orderWideList.toIterator
    }

    orderWideDstream.print(1000)
    // //按比例求分摊: 分摊金额/实际付款金额 = 个数*单价  /原始总金额
    // 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
    //         最后一笔 = 实际付款金额-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
    // 如何判断最后一笔 ？如果当前的一笔 个数*单价== 原始总金额-  Σ其他的明细（个数*单价）
    //   利用 redis(mysql)  保存计算完成的累计值  Σ其他的明细的分摊金额 Σ其他的明细（个数*单价）

    val orderWideAmountDStreame: DStream[OrderWide] = orderWideDstream.mapPartitions { orderWideItr =>
      //建连接
      val jedis: Jedis = RedisUtil.getJedisClient

      val orderWideList: List[OrderWide] = orderWideItr.toList
      println("分区orderIds:" + orderWideList.map(_.order_id).mkString(","))
      //迭代
      for (orderWide <- orderWideList) {
        var splitAmountSum = 0D //其他分摊总金额

        var originAmountSum = 0D //其他原价总金额

        // 要从redis中取得累计值   Σ其他的明细（个数*单价）
        // redis    type?  string    key?  order_origin_sum:[order_id]  value? Σ其他的明细（个数*单价）
        val key = "order_origin_sum:" + orderWide.order_id
        val originAmountSumS: String = jedis.get(key) //其他原价总金额
        //从redis中取出来的任何值都要进行判空
        if (originAmountSumS != null && originAmountSumS.length > 0) {
          //判断
          val originAmountSum = originAmountSumS.toDouble //转换
        }
        //如果等式成立 说明该笔明细是最后一笔  （如果当前的一笔 个数*单价== 原始总金额-  Σ其他的明细（个数*单价））
        val detailOrginAmount: Double = orderWide.sku_num * orderWide.sku_price
        //当前订单个数*单价
        val restOriginAmount: Double = orderWide.original_total_amount - originAmountSum //总原始金额-其他原价总金额
        // 判断计算方式
        //如何判断最后一笔 ? 如果当前的一笔 个数*单价== 原始总金额original_total_amount-  Σ其他的明细（个数*单价）
        // 个数 单价  原始金额 可以从orderWide取到
        if (detailOrginAmount == restOriginAmount) {
          //如果等式成立 说明该笔明细是最后一笔
          // 分摊计算公式 :减法公式  分摊金额= 实际付款金额final_total_amount-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
          // 实际付款金额 在orderWide中，
          // 要从redis中取得  Σ其他的明细的分摊金额
          // redis    type?  string    key?  order_split_sum:[order_id]  value? Σ其他的明细的分摊金额
          val splitAmountSums: String = jedis.get("order_split_sum" + orderWide.order_id) //获取其他分摊总金额
          if (splitAmountSums != null && splitAmountSums.length > 0) {
            splitAmountSum = splitAmountSums.toDouble
          }
          orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - splitAmountSum) * 100D) / 100D //分摊金额=总支付-其他总分摊


        } else {
          //如果不成立
          // 分摊计算公式： 乘除法公式： 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
          //  所有计算要素都在 orderWide 中，直接计算即可
          // 分摊金额计算完成以后
          // 要赋给orderWide中分摊金额
          //  要本次计算的分摊金额 累计到redis    Σ其他的明细的分摊金额
          //   应付金额（单价*个数) 要累计到   Σ其他的明细（个数*单价）
          orderWide.final_detail_amount = orderWide.final_total_amount * detailOrginAmount / orderWide.original_total_amount
          orderWide.final_detail_amount = Math.round(orderWide.final_detail_amount * 100D) / 100D

        }

        // 判断计算方式
        //如何判断最后一笔 ? 如果当前的一笔 个数*单价== 原始总金额original_total_amount-  Σ其他的明细（个数*单价）
        // 个数 单价  原始金额 可以从orderWide取到
        // 要从redis中取得累计值   Σ其他的明细（个数*单价）
        // redis    type?  string    key?  order_origin_sum:[order_id]  value? Σ其他的明细（个数*单价）
        //如果等式成立 说明该笔明细是最后一笔
        // 分摊计算公式 :减法公式  分摊金额= 实际付款金额final_total_amount-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
        // 实际付款金额 在orderWide中，
        // 要从redis中取得  Σ其他的明细的分摊金额
        // redis    type?  string    key?  order_split_sum:[order_id]  value? Σ其他的明细的分摊金额
        //如果不成立
        // 分摊计算公式： 乘除法公式： 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
        //  所有计算要素都在 orderWide 中，直接计算即可
        // 分摊金额计算完成以后
        // 要赋给orderWide中分摊金额
        //  要本次计算的分摊金额 累计到redis    Σ其他的明细的分摊金额
        //   应付金额（单价*个数) 要累计到   Σ其他的明细（个数*单价）

        //合计保存
        splitAmountSum += orderWide.final_detail_amount
        originAmountSum += restOriginAmount
        jedis.setex("order_origin_sum:" + orderWide.order_id, 600, originAmountSum.toString)
        jedis.setex("order_split_sum" + orderWide.order_id, 600, splitAmountSum.toString)
      }

      // 关闭redis
      //返回一个计算完成的 list的迭代器
      jedis.close()
      orderWideList.toIterator
      // 当多个分区的数据 可能会同时访问某一个数值（累计值）的情况 ，可能会出现并发问题
      // 1  本身数据库是否有并发控制 redis 单线程  mysql行锁
      //2   让涉及并发的数据存在于同一个分区中   2.1  进入kafka的时候指定分区键  2.2 在spark 利用dstream[k,v].partitionby(new HashPartitioner(partNum)) shuffle
    }

    val sparkSession: SparkSession = SparkSession.builder().appName("dws_order_wide_app").getOrCreate()
    import sparkSession.implicits._

    orderWideAmountDStreame.foreachRDD { rdd =>
      //TODO 写入clickhouse
      val df = rdd.toDF()
      df.write.mode(SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") //设置事务
        .option("numPartitions", "4") //设置并发
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hadoop102:8123/test0213", "order_wide_0213", new Properties())
      //TODO 写入kafka
      rdd.foreach { orderWide =>
        MyKafkaSink.send("DWS_ORDER_WIDE", JSON.toJSONString(orderWide, new SerializeConfig(true)))

        OffsetManager.saveOffset(orderInfoTopic, orderInfoGroupId, orderInfoOffsetRanges)
        OffsetManager.saveOffset(orderDetailTopic, orderDetailGroupId, orderDetailOffsetRanges)
      }
      ssc.start()
      ssc.awaitTermination()



    }


  }
}