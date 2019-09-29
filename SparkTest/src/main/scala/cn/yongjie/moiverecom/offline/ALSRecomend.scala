package cn.yongjie.moiverecom.offline

import cn.yongjie.moiverecom.model.{Movie, MyRating, User}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession
import cn.yongjie.moiverecom.common.{AppConfig, RedisUtils}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object ALSRecomend {

  val spark = SparkSession
    .builder()
    .master("local[4]")
    .appName("MoiveRecom")
    .getOrCreate()

  def parseMovie(lines: String): Movie = {

    // MovieID::Title::Genres
    val lists = lines.split("::")
    val genres = lists(2).split("\\|")
    Movie(lists(0).toInt, lists(1), genres)
  }

  def parseUser(lines: String): User = {

    //UserID::Gender::Age::Occupation
    val lists = lines.split("::")
    User(lists(0).toInt, lists(1).toCharArray()(0), lists(2).toInt, lists(3).toInt)
  }

  def parseRating(lines: String): MyRating = {

    //UserID::MovieID::Rating::Timestamp
    val lists = lines.split("::")
    MyRating(Rating(lists(0).toInt, lists(1).toInt, lists(2).toDouble), lists(3).toLong)
  }

  def saveUserWatchedMovieToRedis(ratingRDD: RDD[MyRating]): Unit = {

    RedisUtils.init(AppConfig.redisHost, AppConfig.redisPort, AppConfig.redisTimeout,
      AppConfig.redisPassword, AppConfig.redisDatabase)

    val jedis = RedisUtils.getJedis

    // get moives the user have seen and store the results to redis with key equals "UI-UserId"
    ratingRDD
      .map(myRating => (myRating.rating.user, myRating.rating.product))
      .groupByKey()
      .map(item => {
        val userId = item._1.toString
        val key = "UI-" + userId
        val movieList = item._2.toArray[Int]
        jedis.set(key, JSON.toJSONString(movieList, SerializerFeature.PrettyFormat))
      })

    jedis.close()
  }

  def createALSModel(ratingRDD: RDD[MyRating]): Unit = {

    RedisUtils.init(AppConfig.redisHost, AppConfig.redisPort, AppConfig.redisTimeout,
      AppConfig.redisPassword, AppConfig.redisDatabase)

    val jedis = RedisUtils.getJedis

    val model: MatrixFactorizationModel = new ALS()
      .setIterations(10)
      .setRank(10)
      .setLambda(0.01)
      .run(ratingRDD.map(myRating => myRating.rating))

    //analysis similarity of each movie
    val movieSim = model.productFeatures
      .sortByKey(true)
      .map(x => Vectors.dense(x._2))
    val matrixSim = new RowMatrix(movieSim)
    val covariance: Matrix = matrixSim.computeCovariance()
    println("covariance row number" + covariance.numRows + " columns number " + covariance.numCols)



    jedis.close()

  }

  def batchOperation(): Unit = {

    val ratingRDD: RDD[MyRating] = spark
      .sparkContext
      .textFile(AppConfig.ratingsDataPath)
      .map(parseRating).cache()

    // save userId-MoviesList
    saveUserWatchedMovieToRedis(ratingRDD)

    // get similarity of movies
    createALSModel(ratingRDD)

  }

}
