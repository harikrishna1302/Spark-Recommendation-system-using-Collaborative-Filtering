# Spark-Recommendation-system-using-Collaborative-Filtering
## 1. Abstract
This project is about recommending the movies to a user based on the movies that the user have rated before.
The ALS algorithm used in the project is from machine learning library of Apache spark. Using the ALS
algorithm, a Matrix Factorization model is defined. Based on the previously rated data we can predict what
movie will be liked by the user.

## 2. Data set - Movies
The link for the data set is : http://files.grouplens.org/datasets/movielens/ml-1m.zip

## 3. Technologies
Apache Spark-notebook : The Spark open source as the platform to write the programming.  
Scala programming for the implementation.  
Spark Machine Learning : Used the spark machine library learning algorithm for generating the matrix factorization model.  
Comet Cluster (For running the spark-spark notebook) Installing the spark on the comet server and running the notebook on allocated node.

## 4. Implementation
* Installation of spark-notebook on comet supercomputer.  
* Importing the data sets and creating the RDD and data frames Using the match case for the data type used in Apache mllib ALS Algorithm, creating the ratingsRDD to give as input to the Algorithm.  
* Applying the ALS algorithm to the ratings RDD to get the matrix factorization model.  
* Evaluation of the model by finding the Mean Square Error and getting the best model by reducing the MSE.  
* Joining the rating and movies table and sorting the highly rated movies according to the movies rate count.  
* Getting the user movieId recommendations from the ALS algorithm.  
* Making the function to fetch the title of the movie using the recommended movieId in the previous step.  
* Sorting the movies by the predicted ratings and display the names of the movie ratings for the particular user.
