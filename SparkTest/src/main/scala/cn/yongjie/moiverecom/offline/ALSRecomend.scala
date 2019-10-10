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
import org.apache.spark.mllib.stat.Statistics

import scala.collection.mutable

object ALSRecomend {

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

    RedisUtils.init(AppConfig.redisHost,
                    AppConfig.redisPort,
                    AppConfig.redisTimeout,
                    AppConfig.redisPassword,
                    AppConfig.redisDatabase)

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

//    RedisUtils.init(AppConfig.redisHost,
//                    AppConfig.redisPort,
//                    AppConfig.redisTimeout,
//                    AppConfig.redisPassword,
//                    AppConfig.redisDatabase)
//
//    val jedis = RedisUtils.getJedis

    val model: MatrixFactorizationModel = new ALS()
      .setIterations(2)
      .setRank(10)
      .setLambda(0.01)
      .run(ratingRDD.map(myRating => myRating.rating).sample(false, 0.001))

    //analysis similarity of each movie
    val movieSim:RDD[Vector] = model.productFeatures
      .sortByKey()
      .map(x => Vectors.dense(x._2))
    val movieId = model.productFeatures.sortByKey().map(_._1).collect()

    var movieIdMap = new mutable.HashMap[Int, Int]
    for (i <- 0 until movieId.length) {
      movieIdMap.put(i, movieId(i))
    }
//    println(movieIdMap.toList.sortBy(_._1))

    val count = movieSim.count()

    val corrRes = movieSim.cartesian(movieSim)
      .map(x =>{
        val movie01 = x._1;
        val movie02 = x._2;
        getCollaborateSource(movie01, movie02)
      }).collect()

    val arr = Array.ofDim[Double](count.toInt, count.toInt)

    for (i <- 0 until count.toInt) {
      for (j <- 0 until count.toInt) {
        val num = i * count + j
        arr(i)(j) = corrRes(num.toInt)
      }
    }


    for (i <- 0 until arr.length) {
      val simArr = arr(i)
      val movieId = movieIdMap.get(i).get
      val key = "UU-" + movieId
      val sim5MovieId = getTop5SimMovieId(simArr, movieIdMap)
      println(key + "top5 movieId: " + sim5MovieId)
//      jedis.set(key, JSON.toJSONString(sim5MovieId, SerializerFeature.PrettyFormat))
    }

//    println(arr.foreach(x => println(x.mkString(","))))
//    jedis.close()

  }

  def batchOperation(): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("MoiveRecom")
      .getOrCreate()

    val ratingRDD: RDD[MyRating] = spark
      .sparkContext
      .textFile(AppConfig.ratingsDataPath)
      .map(parseRating).cache()

    // save userId-MoviesList
//    saveUserWatchedMovieToRedis(ratingRDD)

    // get similarity of movies
    createALSModel(ratingRDD)

  }

  def getCollaborateSource(movie01: Vector, movie02:Vector): Double ={

    val numerator = movie01.toArray.zip(movie02.toArray).map(x => x._1 * x._2).sum.toDouble
    val movie01Var = movie01.toArray.map(x => math.pow(x, 2)).reduce(_ + _).toDouble
    val movie02Var = movie02.toArray.map(x => math.pow(x, 2)).sum.toDouble
    val corr = numerator / (math.sqrt(movie01Var) + math.sqrt(movie02Var))
    corr
  }

  def getTop5SimMovieId(corrArr: Array[Double], movieIdMap: mutable.HashMap[Int, Int]): Unit = {

    val tempId = corrArr.zip(Array.range(0, corrArr.length)).sortBy(_._1).reverse.map(_._2).take(5)
    val movieId = new Array[Int](5)
    for (i <- 0 until 5) {
      movieId(i) = movieIdMap.get(tempId(i)).get
    }
    println(movieId.mkString(","))
    movieId
  }

}
