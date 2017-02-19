
object Cells {
  sc

  /* ... new cell ... */

  import org.apache.spark.sql._

  /* ... new cell ... */

  val sqlContext = new SQLContext(sc)

  /* ... new cell ... */

  import sqlContext.implicits._

  /* ... new cell ... */

  import org.apache.spark.mllib.recommendation.ALS

  /* ... new cell ... */

  import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

  /* ... new cell ... */

  import org.apache.spark.mllib.recommendation.Rating

  /* ... new cell ... */

  val rate = sc.textFile("ratings.dat")

  /* ... new cell ... */

  val rating = rate.map(_.split("::") match { case Array(user, item, rate, timestamp) => Rating(user.toInt, item.toInt, rate.toDouble)})

  /* ... new cell ... */

  val numRatings = rating.count()

  /* ... new cell ... */

  val numRatedMovies = rating.map(_.product).distinct().count()

  /* ... new cell ... */

  val numRatingUsers = rating.map(_.user).distinct().count()

  /* ... new cell ... */

  val ratingsDF = rating.toDF("userId","movieId","rating")

  /* ... new cell ... */

  ratingsDF.registerTempTable("ratings")

  /* ... new cell ... */

  val moviesRDD = sc.textFile("movies.dat")

  /* ... new cell ... */

  val movies = moviesRDD.map(r=>r.split("::")).map(r=>(r(0).toInt, r(1).toString, r(2).toString ))

  /* ... new cell ... */

  val moviesDF = movies.toDF("movieId", "title", "genre")

  /* ... new cell ... */

  moviesDF.registerTempTable("movies")

  /* ... new cell ... */

  val usersRDD = sc.textFile("users.dat")

  /* ... new cell ... */

  val users = usersRDD.map(r=>r.split("::")).map(r=>(r(0).toInt, r(1).toString, r(3).toInt))

  /* ... new cell ... */

  val usersDF = users.toDF("userId","gender","zip")

  /* ... new cell ... */

  usersDF.registerTempTable("users")

  /* ... new cell ... */

  val top_rated = sqlContext.sql("SELECT movies.title, mr.maxr, mr.minr, mr.cntu FROM(SELECT ratings.movieId, max(ratings.rating)as maxr, min(ratings.rating) as minr, count(distinct userId) as cntu FROM ratings GROUP BY ratings.movieId) mr join movies on mr.movieId = movies.movieID order by mr.cntu desc")

  /* ... new cell ... */

  top_rated.show()

  /* ... new cell ... */

  val rank = 70

  /* ... new cell ... */

  val numIterations = 20

  /* ... new cell ... */

  val lambda = 0.01

  /* ... new cell ... */

  val model = ALS.train(rating, rank, numIterations, lambda)

  /* ... new cell ... */

  val usersProducts = rating.map { case Rating(user, product, rate) => (user, product)}

  /* ... new cell ... */

  val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate) }

  /* ... new cell ... */

  val ratesAndPreds = rating.map { case Rating(user, product, rate) => ((user, product), rate)}.join(predictions)

  /* ... new cell ... */

  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
    val err = (r1 - r2)
    err * err
  }.mean()

  /* ... new cell ... */

  println("Mean Squared Error = " + MSE)

  /* ... new cell ... */

  val reco = model.recommendProducts(25,10)

  /* ... new cell ... */

  println(reco.mkString("\n"))

  /* ... new cell ... */

  val titles = moviesDF.rdd.map(array => (array(0),array(1))).collectAsMap()

  /* ... new cell ... */

  val res1 = reco.sortBy(-_.rating).take(10).map(rating => (rating.user,rating.product,titles(rating.product))).foreach(println)
}
                  