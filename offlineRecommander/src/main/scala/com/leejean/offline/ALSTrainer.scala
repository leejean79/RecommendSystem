package com.leejean.offline

import breeze.numerics.sqrt
import com.leejean.offline.OfflineRecommander.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
模型参数训练
 */

//ratings.csv
/*
1,                    用户ID
31,                   电影ID
2.5,                  评分
1260759144            评分时间戳
 */
case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)
/**
 * MongoDB 配置对象
 *
 * @param uri MongoDB连接地址
 * @param db  操作的MongoDB数据库
 */
case class MongoConfig(val uri: String, val db: String)

/*
该类的目的是寻找最优的参数组合 (rank, iteration, lambda)
 */

object ALSTrainer {

  def getRmse(model:MatrixFactorizationModel, trainData:RDD[Rating]):Double={
    //需要构造一个usersProducts  RDD[(Int,Int)]
    val userMovies = trainData.map(item => (item.user,item.product))
    //得到评分预测值
    val predictRating = model.predict(userMovies)

    //将真实值、预测值转化成统一的格式
    val real = trainData.map(item => ((item.user,item.product),item.rating))
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))

    //计算真实值与预测值的均方差
    sqrt(
      real.join(predict).map{case ((uid,mid),(real,pre))=>
        // 真实值和预测值之间的两个差值
        val err = real - pre
        err * err
      }.mean()
    )
  }

  // 输出最终的最优参数
  def adjustALSParams(trainData:RDD[Rating]): Unit ={
    val result = for(rank <- Array(30,40,50,60,70); lambda <- Array(1, 0.1, 0.001))
      yield {
        val model = ALS.train(trainData,rank,5,lambda)
        val rmse = getRmse(model,trainData)
        (rank,lambda,rmse)
      }
    println(result.sortBy(_._3).head)
  }

  def main(args: Array[String]): Unit = {

    //创建sparkconf
  val config: Map[String, String] = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://f5903:27017//recommender",
    "mongo.db" -> "recommender"
  )
    val conf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")

    //创建sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读取mongodb数据
    //创建mongodb的config
    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._
    //加载评分数据
    val ratingRdd = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      //直接转化成Mlib中自带的Rating： A more compact class to represent a rating than Tuple3[Int, Int, Double].
      .rdd.map(rating => Rating(rating.uid, rating.mid, rating.score)).cache()

    //输出最优参数
    adjustALSParams(ratingRdd)

    //关闭spark
    spark.close()


  }



}
