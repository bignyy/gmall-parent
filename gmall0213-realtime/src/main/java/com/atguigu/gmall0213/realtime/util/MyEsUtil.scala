package com.atguigu.gmall0213.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import java.util

object MyEsUtil {
  private var factory:JestClientFactory=null;
  def getJestClient():JestClient={
    if(factory!=null){
      factory.getObject
    }else{
      build()
      factory.getObject
    }
  }
  def build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200" )
      .multiThreaded(true)
        .maxTotalConnection(20)
        .connTimeout(10000).build()
    )

  }
  //单条写入，IO频繁，产生较多segment
  def saveToEs(): Unit ={
    val jestClient=getJestClient()
    // 写操作
    val index=new Index.Builder(Movie("0103","复仇者联盟")).index("movie_test0213_20200721").`type`("_doc").id("0213").build()
    jestClient.execute(index)
    jestClient.close()
  }

  /**
    * 批次化操作  batch   Bulk
    * @param list 传入的封装数据Any =>doc,string=>id
    * @param indexName 索引名字
    */
  def bulkSave(list: List[(Any, String)], indexName: String)={
    if (list != null && list.size > 0) {
      val jestClient: JestClient = getJestClient()
      val bulkBuilder = new Bulk.Builder
      bulkBuilder.defaultIndex(indexName).defaultType("_doc")
      for ((doc, id) <- list) {
        val index = new Index.Builder(doc).id(id).build() //如果给id指定id 幂等性（保证精确一次消费的必要条件） //不指定id 随机生成 非幂等性
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulk).getItems
      println("已保存" + items.size())

      jestClient.close()
    }
  }


  def queryFromEs(): Unit ={
    val jestClient=getJestClient()
    val query="{\n \"query\": {\n    \"match\": {\n      \"name\": \"red\"\n    }\n  },\n  \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"asc\"\n      }\n    }\n  ],\n    \"from\": 0,\n  \"size\": 20 \n\n}";
    val searchSourceBuilder  = new SearchSourceBuilder
    searchSourceBuilder.query(new MatchQueryBuilder("name","red"))
    searchSourceBuilder.sort("doubanScore",SortOrder.ASC)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(20)
    val query2 = searchSourceBuilder.toString
    println(query2)
    val search = new Search.Builder(query2).addIndex("movie_index0213").addType("movie").build()
    val result = jestClient.execute(search)
    val resultList: util.List[SearchResult#Hit[util.Map[String, Object], Void]] = result.getHits(classOf[ util.Map[String,Object]])

    import scala.collection.JavaConversions._
    for (hit <- resultList){
      val source = hit.source
      println(source)
    }
    jestClient.close()



  }



  def main(args: Array[String]): Unit = {
    //saveToEs()
    // query 操作
    queryFromEs()
  }
  case class Movie(id:String ,movie_name:String)
}
