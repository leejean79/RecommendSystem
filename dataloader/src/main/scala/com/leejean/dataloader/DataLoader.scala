package com.leejean.dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

//movie.csv
//25^                                     电影ID
// Leaving Las Vegas (1995)^              片名
// Someone does a nasty hatchet job^      电影简介
// 112 minutes^                           电影时长
// January 27, 1998^
// 1995^
// English ^
// Drama|Romance ^
// Nicolas Cage|Elisabeth Shue|....^
// Mike Figgis
case class Movie(val mid: Int, val name: String, val genres: String)


//ratings.csv
/*
1,                    用户ID
31,                   电影ID
2.5,                  评分
1260759144            评分时间戳
 */
case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

//tags.csv
/*
15,                           用户ID
339,                          电影ID
sandra 'boring' bullock,      tag
1138537770                     tag时间戳
 */
case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
 * MongoDB 配置对象
 *
 * @param uri MongoDB连接地址
 * @param db  操作的MongoDB数据库
 */
case class MongoConfig(val uri: String, val db: String)

/**
 * ElasticSearch 配置对象
 *
 * @param httpHosts      ES的地址
 * @param transportHosts ES的通讯端口
 * @param index          ES操作的Index
 * @param clusterName    ES的集群名称
 */
case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clusterName: String)





object DataLoader {

  // Moive在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"

  // Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"

  // Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  // Movie在ElasticSearch中的Index名称
  val ES_MOVIE_TYPE_NAME = "Movie"

  // Tag在ElasticSearch中的Index名称
//  val ES_TAG_TYPE_NAME = "Tag"

  def saveInMongodb(movies: DataFrame, ratings: DataFrame, tags: DataFrame)(implicit mongoConf: MongoConfig): Unit = {

    val client: MongoClient = MongoClient(MongoClientURI(mongoConf.uri))

    //初始化，删除已存在的collection
    client(mongoConf.db)(MOVIES_COLLECTION_NAME).dropCollection()
    client(mongoConf.db)(TAGS_COLLECTION_NAME).dropCollection()
    client(mongoConf.db)(RATINGS_COLLECTION_NAME).dropCollection()

    //写入数据
    movies
      .write
      .option("uri", mongoConf.uri)
      .option("collection", MOVIES_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratings
      .write.option("uri", mongoConf.uri)
      .option("collection", RATINGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tags
      .write.option("uri", mongoConf.uri)
      .option("collection", TAGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //建立索引，快速查询
    client(mongoConf.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    client(mongoConf.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    client(mongoConf.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    client(mongoConf.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    client(mongoConf.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))

    //关闭数据库连接
    client.close()

  }

  def saveInEs(esMoives:DataFrame)(implicit esConfig:ESConfig): Unit = {

    //新建es配置信息
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()
    //建立es客户端
    val esClient: PreBuiltTransportClient = new PreBuiltTransportClient(settings)
      //将es集群的所有es.transportHosts加入esCLient
      //利用正则提取所有节点的名称可端口
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String, port:String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }
    //清除es中已有数据
    if(esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }
    //创建新的index
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))
    //将数据写入es
    esMoives
      .write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      //保存时要指明路径
      .save(esConfig.index+"/"+ES_MOVIE_TYPE_NAME)

  }

  def main(args: Array[String]): Unit = {

    val conig = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://f5903:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "f5903:9200",
      "es.transportHosts" -> "f5903:9300",
      "es.cluster.name" -> "es-cluster",
      "es.index" -> "recommendation"
    )

    //文件地址
    val MOVIE_DATA_PATH = "E:\\ideaProjects\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PATH = "E:\\ideaProjects\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\ratings1.csv"
    val TAG_DATA_PATH = "E:\\ideaProjects\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\tags1.csv"

    // 定义MongoDB的配置对象
    implicit val mongoConf = new MongoConfig(conig("mongo.uri").asInstanceOf[String], conig("mongo.db").asInstanceOf[String])
    //定义es的配置对象
    implicit val esConfig = new ESConfig(conig("es.httpHosts").asInstanceOf[String],  conig("es.transportHosts").asInstanceOf[String], conig("es.index").asInstanceOf[String], conig("es.cluster.name").asInstanceOf[String])

    //定义sparkconf
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(conig.get("spark.cores").get)
    //定义sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //隐式转换
    import spark.implicits._

    //读取本地文件形成rdd，转为df
    val movieRdd = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF: DataFrame = movieRdd.map {
      line => {
        val x = line.split(",")
        //Movie(x(0).trim.toInt, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim.split("\\|"), x(7).trim.split("\\|"), x(8).trim.split("\\|"), x(9).trim.split("\\|"))
        Movie(x(0).trim.toInt, x(1).trim, x(2).trim)
      }
    }.toDF()

    val ratingRdd = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF: DataFrame = ratingRdd.map {
      line => {
        val x = line.split(",")
        Rating(x(0).toInt, x(1).toInt, x(2).toDouble, x(3).toInt)
      }
    }.toDF()

    val tagRdd = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRdd.map(line => {
      val x = line.split(",")
      Tag(x(0).toInt, x(1).toInt, x(2).toString, x(3).toInt)
    }).toDF()

    //将rdd保存到mongodb中
    saveInMongodb(movieDF, ratingDF, tagDF)

    //将数据保存到es
    //数据格式：mid，tag1|tag2|..

    //1引入sparksql内置函数库
    import org.apache.spark.sql.functions._
    //2对tagDF以mid进行聚合,并将标签拼接在一起
    val tagCollectDF: DataFrame = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set( $"tag")).as("tags"))
    //3 movieDF左join tagCollectDF
    val esMovieDF = movieDF.join(tagCollectDF, Seq("mid", "mid"), "left").select("mid","name","genres","tags")
   //4 保存到es
    saveInEs(esMovieDF)

    //关闭sparkcontext
    spark.close()
  }

}
