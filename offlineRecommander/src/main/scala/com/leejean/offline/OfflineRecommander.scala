package com.leejean.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.jblas.DoubleMatrix


case class Movie(val mid: Int, val name: String, val genres: String)


//ratings.csv
/*
1,                    用户ID
31,                   电影ID
2.5,                  评分
1260759144            评分时间戳
 */
case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

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

//推荐结果样例类mid, score
case class Recommandationg(rid:Int, r:Double)
//基于用户推荐样例类uid，Seq[Recommandationg]
case class UserRecs(uid:Int, recs: Seq[Recommandationg])
//基于电影推荐样例类mid，Seq[Recommandationg]
case class MovieRecs(mid:Int, recs: Seq[Recommandationg])

object OfflineRecommander {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MAX_NUMS_RECS = 20
  val USER_RECOMMANDATIION = "UserRecommandation"
  val MOVIE_RECOMMANDATION = "MovieRecommandation"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",// spark://f5903:7077, local[*]
      "mongo.uri" -> "mongodb://f5903:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建conf
    val conf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommader")
      .set("spark.executor.memory", "5G")
//      .set("spark.submit.deployMode", "client")
      .set("spark.driver.memory", "2G")
      // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
//      .setJars(List("E:\\ideaProjects\\RecommendSystem\\recommender\\dataloader\\target\\dataloader-1.0-SNAPSHOT.jar",
//        "E:\\ideaProjects\\RecommendSystem\\recommender\\offlineRecommander\\target\\offlineRecommander-1.0-SNAPSHOT.jar"
//      ))
//      .setJars()

    //创建sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读取mongodb数据
    //创建mongodb的config
    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    val ratingRdd: RDD[(Int, Int, Double)] = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd.map(rating => (rating.uid, rating.mid, rating.score)).cache()

    //电影数据集
    val midRdd: RDD[Int] = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.sp ark.sql")
      .load()
      .as[Movie]
      .rdd.map(_.mid).cache()

    //训练ALS模型
    //创建训练数据集trainData
    val trainData: RDD[Rating] = ratingRdd.map(rating => Rating(rating._1, rating._2, rating._3))
    /*
       * @param ratings    RDD of [[Rating]] objects with userID, productID, and rating
      * @param rank       number of features to use
      * @param iterations number of iterations of ALS
        */
    val (rank, iteration, lambda) = (50, 10, 0.01)

    // 训练后的模型：
    // model包含：userFeatures: RDD[(Int, Array[Double])]，the userId and
    // *                     the features computed for this user.
    //            productFeatures: RDD[(Int, Array[Double])]) the productId
    // *                        and the features computed for this product
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iteration, lambda)


    //计算用户推荐矩阵
    //创建usersProductsRdd
    val usrRdd: RDD[Int] = ratingRdd.map(rating => rating._1)
    val userMovieRdd: RDD[(Int, Int)] = usrRdd.cartesian(midRdd)

    //预测矩阵
    val preRatings: RDD[Rating] = model.predict(userMovieRdd)

    //对结果取前20，并进行封装
    val userRatingRecDF: DataFrame = preRatings.map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, iter) => (uid, iter.toList.sortWith(_._2 > _._2).take(MAX_NUMS_RECS)
          .map(row => Recommandationg(row._1, row._2))
        )
      }.toDF()

    userRatingRecDF
        .write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECOMMANDATIION)
      .format("com.mongodb.spark.sql")
      .mode("overwrite")
      .save()


    /*
    计算电影相似度矩阵
     */
    //获取电影特征,并封装到矩阵中(scala.Int, scala.Array[scala.Double])
    // productFeatures RDD of tuples where each tuple represents the productId
    // *                        and the features computed for this product.
    val movieFeatsMatrixRdd: RDD[(Int, DoubleMatrix)] = model.productFeatures.map{case(mid, feats) => (mid, new DoubleMatrix(feats))}

    //对所有不相同电影进行笛卡尔积，并封装
    val movieRecDf: DataFrame = movieFeatsMatrixRdd.cartesian(movieFeatsMatrixRdd)
      //过滤相同mid的
      .filter { case (movie1, movie2) => movie1._1 != movie2._1 }
      //计算余弦相似度
      .map { case (movie1, movie2) =>
        val movieSim = this.cosinSim(movie1._2, movie2._2)
        (movie1._1, (movie2._1, movieSim))
      }
      //过滤掉相似度<0.6的
      .filter(_._2._2 > 0.6)
      //分组
      .groupByKey()
      //包装
      .map { case (mid, tuples) => MovieRecs(mid, tuples.toList.map(x => Recommandationg(x._1, x._2))) }
      .toDF()

    movieRecDf
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECOMMANDATION)
      .format("com.mongodb.spark.sql")
      .mode("overwrite")
      .save()

    //关闭spark
    spark.close()

  }

  //计算余弦相似度函数
  def cosinSim(a: DoubleMatrix, b: DoubleMatrix): Double = {
    a.dot(b) / (a.norm2() * b.norm2())
  }

}
