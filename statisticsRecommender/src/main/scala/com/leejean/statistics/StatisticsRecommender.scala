package com.leejean.statistics

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

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
 * 推荐对象
 * rid：推荐的movie mid
 * r：movie的评分
 */
case class Recommendation(rid:Int, r:Double)

/**
 *电影类别推荐
 * genres:电影类别
 * recs：推荐的电影
 */
case class GenresRecommedation(genres:String, recs:Seq[Recommendation])


object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"

  //统计结果表名
  val RATE_NUMBERS_MOVIES = "rateNumbersMovie"
  val RATE_NUMBERS_RECENT_MOVIE = "rateNumberRecentMovie"
  val AVERAGE_SCORE_MOVIES = "averageScoreMovies"
  val GENRES_TOP_MOVIES = "genresTopMovies"


  //入口方法
  def main(args: Array[String]): Unit = {

    val configMap = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://f5903:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val mongoConfig = MongoConfig(configMap("mongo.uri"), configMap("mongo.db"))


    //创建sparkconf
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(configMap("spark.cores"))
    //创建sparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //加载数据
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()


    //创建临时表
    ratingDF.createOrReplaceTempView("ratings")

    /*
    统计所有历史数据中每个电影的评分数
     */
    //表结构：mid，count
    val rateCountMovieDF: DataFrame = spark.sql("select mid, count(mid) as count from ratings group by mid")

    rateCountMovieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_NUMBERS_MOVIES)
      .format("com.mongodb.spark.sql")
      .mode("overwrite")
      .save()

    /*
    统计以月为单位每个电影的评分个数
     */
    //表结构：mid，count， time

    //格式化日期
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注：本项目数据集时间戳精确到秒，所以要乘以1000，化为毫秒(1260759179 => 202009)
    spark.udf.register("changeDate", (x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    //对数据进行转换
    val tempDF: DataFrame = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")

    tempDF.createOrReplaceTempView("ratingCountByMonth")

    //以时间和mid对数据进行count
    val ratingCountByDateDF: DataFrame = spark.sql("select mid, count(mid) as count, yearmonth from ratingCountByMonth group by yearmonth, mid")

    ratingCountByDateDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_NUMBERS_RECENT_MOVIE)
      .format("com.mongodb.spark.sql")
      .mode("overwrite")
      .save()

    /*
    统计每个电影的平均评分
     */
    val aveRatingMovieDF: DataFrame = spark.sql("select mid, avg(score) as avg from ratings group by mid")

    aveRatingMovieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", AVERAGE_SCORE_MOVIES)
      .format("com.mongodb.spark.sql")
      .mode("overwrite")
      .save()

    /*
    统计每种电影类别中评分的top10
     */

    //movie数据增加平均评分
    val movieWithScoreDF: DataFrame = movieDF.join(aveRatingMovieDF, Seq("mid", "mid"))

    //所有电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    val genresRdd: RDD[String] = spark.sparkContext.makeRDD(genres)

    //做笛卡尔积
    //过滤
    //提取需要字段重新封装
    val genreTop10MovieDF: DataFrame = genresRdd.cartesian(movieWithScoreDF.rdd).filter {
      case (genres, row) => row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
    }.map {
      case (genres, row) => {
        (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
      }
    }.groupByKey().map {
      //排序，取前十
      case (genres, iters) => iters.toList.sortWith(_._2 > _._2).take(10)
        //封装进样例类
        .map(item => Recommendation(item._1, item._2))
    }.toDF()

    genreTop10MovieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", GENRES_TOP_MOVIES)
      .format("com.mongodb.spark.sql")
      .mode("overwrite")
      .save()



    //关闭spark
    spark.stop()



  }

}
