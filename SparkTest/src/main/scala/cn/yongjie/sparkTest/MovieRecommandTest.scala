package cn.yongjie.sparkTest

import java.io.{BufferedReader, File, InputStreamReader}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.mllib.evaluation.RegressionMetrics


case class Movie(movieId: Int, title: String, genrec: Array[String])

case class User(userId: Int, gender: String, age: Int, occupation: Int, zipCode: String)


object MovieRecommandTest {

  /*
  DataSet description

  ratings.dat
    UserID::MovieID::Rating::Timestamp

    - UserIDs range between 1 and 6040
    - MovieIDs range between 1 and 3952
    - Ratings are made on a 5-star scale (whole-star ratings only)
    - Timestamp is represented in seconds since the epoch as returned by time(2)
    - Each user has at least 20 ratings

   users.dat
    UserID::Gender::Age::Occupation::Zip-code

    - Gender is denoted by a "M" for male and "F" for female
    - Age is chosen from the following ranges:

      *  1:  "Under 18"  * 18:  "18-24" * 25:  "25-34"    * 35:  "35-44"
      *  * 45:  "45-49"  * 50:  "50-55" * 56:  "56+"

    - Occupation is chosen from the following choices:

      *  0:  "other" or not specified        *  1:  "academic/educator"        *  2:  "artist"
      *  3:  "clerical/admin"        *  4:  "college/grad student"        *  5:  "customer service"
      *  6:  "doctor/health care"        *  7:  "executive/managerial"        *  8:  "farmer"
      *  9:  "homemaker"        * 10:  "K-12 student"        * 11:  "lawyer"
      * 12:  "programmer"        * 13:  "retired"        * 14:  "sales/marketing"
      * 15:  "scientist"        * 16:  "self-employed"        * 17:  "technician/engineer"
      * 18:  "tradesman/craftsman"        * 19:  "unemployed"        * 20:  "writer"


   movies.dat
    MovieID::Title::Genres

    - Titles are identical to titles provided by the IMDB (including year of release)
    - Genres are pipe-separated and are selected from the following genres:

      * Action        * Adventure        * Animation        * Children's        * Comedy
      * Crime        * Documentary        * Drama        * Fantasy        * Film-Noir
      * Horror        * Musical        * Mystery        * Romance        * Sci-Fi
      * * Thriller        * War        * Western
   */

  //1. Define a rating elicitation function
  //Movie id, title
  def ratingElicaitaion(movies: Seq[(Int, String)]) = {

    val prompt = "Please rate the following movie(1-5(best) or 0 if not seen: )"
    println(prompt)

    val ratings = movies.flatMap { movie => {
      var rating: Option[Rating] = None
      var valid = false
      while (!false) {
        println(movie._2 + ":")
        try {
          val buffer = new BufferedReader(new InputStreamReader(System.in))
          val rate = buffer.readLine().toDouble
          buffer.close()
          if (rate > 5 || rate < 0) {
            println(prompt)
          } else {
            valid = true
            if (rate > 0) {
              rating = Some(Rating(0, movie._1, rate))
            }
          }
        } catch {
          case e: Exception => println(prompt)
        }
      }
      rating match {
        case Some(r) => Iterator(r)
        case None => Iterator.empty
      }
    }
    }
    if (ratings.isEmpty) {
      error("No rating")
    } else {
      ratings
    }
  }

  //2. Define a RMSE computation function
  def computeRMSE(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val pred = model.predict(data.map(rating => (rating.user, rating.product)))
    val predRealRdd = data
      .map(rat => (rat.user + "_" + rat.product, rat.rating))
      .join(pred.map(rat => (rat.user + "_" + rat.product, rat.rating))).values

    new RegressionMetrics(predRealRdd).rootMeanSquaredError

  }


  //3. Main
  def main(args: Array[String]): Unit = {


    //3.1 Setup env
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

//    if (args.length != 1) {
//      println("Need base file path")
//      sys.exit(1)
//    }

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("MovieRecommendTest")
      .getOrCreate()

    //3.2 Load ratings data and know your data
    val moviePath = "data/ml-1m"//args(0)

    val ratingRDD = spark.sparkContext.textFile(new File(moviePath, "ratings.dat").toString)
      .map(row => {
        val splitList = row.split("::")
        Rating(splitList(0).toInt, splitList(1).toInt, splitList(2).toDouble)
      })

    val userRDD = spark.sparkContext.textFile(new File(moviePath, "users.dat").toString)
      .map(row => {
        val splitList = row.split("::")
        User(splitList(0).toInt, splitList(1), splitList(2).toInt, splitList(3).toInt, splitList(4))
      })

    val movieRDD = spark.sparkContext.textFile(new File(moviePath, "movies.dat").toString)
      .map(row => {
        val splitList = row.split("::")
        Movie(splitList(0).toInt, splitList(1), splitList(2).split("\\|"))
      })

    //3.3 Elicitate personal rating
    //find top 50 popular movie
    import spark.implicits._
    ratingRDD.toDF().createOrReplaceTempView("tb_rating")
    movieRDD.toDF().createOrReplaceTempView("tb_movie")

//    val randomFavourMovies = spark.sql(
//      """
//        |SELECT
//        | product,
//        | title
//        |FROM
//        | (SELECT
//        |   product,
//        |   COUNT(product) AS cnt
//        | FROM
//        |   tb_rating
//        | GROUP BY
//        |   product
//        | ORDER BY
//        |   cnt DESC
//        | LIMIT
//        |   50) AS b
//        |LEFT JOIN
//        | tb_movie AS c
//        |ON b.product=c.movieId
//      """.stripMargin).sample(false, 0.2, 0).map(row => {
//      val movieId = row.getAs[Int]("product")
//      val title = row.getAs[String]("title")
//      (movieId, title)
//    }).collect()

//    val userRating: Seq[Rating] = ratingElicaitaion(randomFavourMovies)
//    val userRatingRDD = spark.sparkContext.parallelize(userRating)


    //3.4 Split data into train(60%), validation(20%) and test(20%)
    val splitArray = ratingRDD.randomSplit(Array(0.6, 0.2, 0.2), 0)
    val trainingSet = splitArray(0).persist()
    val validationSet = splitArray(1).persist()
    val testingSet = splitArray(2).persist()


    //3.5 Train model and optimize model with validation set

    val rankList = List(6, 10)
    val iterationList = List(4, 8)
    val lambdaList = List(0.01, 0.02)

    var bestModel: Option[MatrixFactorizationModel] = None
    var bestRMSE = Double.MaxValue
    var bestRank = -1
    var bestIter = 0
    var bestLambda = -1.0

    for (rank <- rankList; iter <- iterationList; lambda <- lambdaList) {
      val model = ALS.train(trainingSet, rank = rank, iterations = iter, lambda = lambda)
      val rmse = computeRMSE(model, validationSet)
      println("RMSE(validation) = "+rmse+" with ranks="+rank+", iter="+iter+", Lambda="+lambda)

      if (rmse < bestRMSE) {
        bestModel = Some(model)
        bestRMSE = rmse
        bestRank = rank
        bestIter = iter
        bestLambda = lambda
      }
    }

    //3.6 Evaluate model on test set
    val testRmse = computeRMSE(bestModel.get, testingSet)
    println("The best model was trained with rank="+bestRank+", Iter="+bestIter+", Lambda="+bestLambda+
      " and compute RMSE on test is "+testRmse)

    //3.7 Create a baseline and compare it with best model
    val meanRating = trainingSet.union(validationSet).map(_.rating).mean()
    val baseRmse = trainingSet.union(validationSet)
      .map(_.rating)
      .map(x => Math.pow(x - meanRating, 2))
      .mean()
    val improvement = (baseRmse - testRmse) / baseRmse * 100
    println(s"improve rate is %1.2f".format(improvement))

    //3.8 Make a personal recommendation


  }

  def recommendTest01(): Unit = {

    /*
    To Do List
    数据：MovieLens 1M数据集
    需求：
    统计ratings, movies和users的数量
    找到最活跃的用户，并找出此用户对哪些电影评分高于4
    使用ALS构建电影评分模型
    预测结果并评估模型
    */
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("MovieRecommendTest")
      .getOrCreate()

    //导入隐式转换，这样rdd和df，ds之间可以进行转换
    import spark.implicits._

    // 1::1193::5::978300760
    val ratingRdd = spark.sparkContext
      .textFile("data/ml-1m/ratings.dat")
      .map(row => {
        val splitList = row.split("::")
        Rating(splitList(0).toInt, splitList(1).toInt, splitList(2).toDouble)
      })
    val ratingDf = ratingRdd.toDF()

    //1::F::1::10::48067
    val userDf = spark.sparkContext
      .textFile("data/ml-1m/users.dat")
      .map(row => {
        val splitList = row.split("::")
        User(splitList(0).toInt, splitList(1), splitList(2).toInt, splitList(3).toInt, splitList(4))
      }).toDF()

    //1::Toy Story (1995)::Animation|Children's|Comedy
    val movieDf = spark.sparkContext
      .textFile("data/ml-1m/movies.dat")
      .map(row => {
        val splitList = row.split("::")
        Movie(splitList(0).toInt, splitList(1), splitList(2).split("\\|"))
      }).toDF()

    // 统计ratings, movies和users的数量
    //    println("rating muber: " + ratingDf.count())
    //    println("user muber: " + userDf.count())
    //    println("movie muber: " + movieDf.count())

    /*
    找到最活跃的用户，并找出此用户对哪些电影评分高于4
     */
    ratingDf.createOrReplaceTempView("tb_rating")
    userDf.createOrReplaceTempView("tb_user")
    movieDf.createOrReplaceTempView("tb_movie")

    // result:
    //    val mostViewUserDf = spark.sql(
    //      """
    //        |SELECT
    //        | userId,
    //        | COUNT(DISTINCT(movieId)) as movieCnt
    //        |FROM
    //        | tb_rating
    //        |GROUP BY
    //        | userId
    //        |ORDER BY
    //        | movieCnt
    //        |DESC
    //        |limit 10
    //      """.stripMargin)
    //    mostViewUserDf.show()

    //    val movieRecommendByMostViewUserDf = spark.sql(
    //      """
    //        |SELECT
    //        | a.movieId,
    //        | b.genrec
    //        |FROM
    //        | (SELECT
    //        |   movieId
    //        | FROM
    //        |   tb_rating
    //        | WHERE
    //        |   userId=4169
    //        | And
    //        |   rating>4) as a
    //        | LEFT JOIN
    //        |   tb_movie as b
    //        | ON
    //        |   a.movieId=b.movieId
    //      """.stripMargin)
    //
    //    movieRecommendByMostViewUserDf.show()


    // 使用ALS构建电影评分模型

    /*
    split training data and testing data
     */
    val trainTestSplit: Array[RDD[Rating]] = ratingRdd.randomSplit(Array(0.8, 0.2), 0L)
    val trainingSet: RDD[Rating] = trainTestSplit(0).cache()
    val testingSet: RDD[Rating] = trainTestSplit(1).cache()

    val model: MatrixFactorizationModel = new ALS().setRank(20).setIterations(10).run(trainingSet)

    /*
    recommend top 5 score movie by user 4169
     */
    //    val recommendProducts = model.recommendProducts(4169, 5)
    //    val movieIdTitleMap = movieDf.rdd.map(row => {
    //      val movieId = row.getAs[Int]("movieId")
    //      val title = row.getAs[String]("title")
    //      (movieId, title)
    //    }).collectAsMap()
    //
    //    recommendProducts.map(rating =>{
    //      (rating.product, movieIdTitleMap(rating.product), rating.rating)
    //    }).foreach(println)


    /*
    evaluate the performance of the trained model
     */

    // method 1  calculate mean absolute error  calculate confuse matrix
    val testPred = model.predict(testingSet.map(rating => (rating.user, rating.product)))
      .map(rating => {
        val user = rating.user
        val product = rating.product
        val userProd = user + "_" + product
        (userProd, rating.rating)
      })

    val testReal = testingSet.map(rating => {
      val user = rating.user
      val product = rating.product
      val userProd = user + "_" + product
      (userProd, rating.rating)
    })

    //res 0.7221753949977137
    //    val mae = testPred.join(testReal).map(pair =>{
    //      math.abs(pair._2._1 - pair._2._2)
    //    }).mean()

    //    // false positive
    //    val fp = testPred.join(testReal).filter{case(key, (pred, real)) => pred >=4 & real <=1}
    //    println("fp" + fp.count())
    //    // false negetive
    //    val fn = testPred.join(testReal).filter{case(key, (pred, real)) => pred <=1 & real >=4}
    //    println("fn" + fn.count())


    // method2 use RegressionMetrics
    val metrics = new RegressionMetrics(testPred.join(testReal).values)
    println(metrics.meanAbsoluteError)
  }
}
