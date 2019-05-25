Spark project was uploaded to this directory and it was implemented to answer the following Requirements. 

Steps to run the code: 

----- to run this code ------
1- install java
2- install spark framework on your pc - follow steps on the spark website.
3- inside the project folder, run the command: spark-shell
4- make sure the FIFA-18-Video-Game-Player-Stats.csv file is exist in the same dir of  fifa-18-spark.scala
5- run command: :load fifa-18-spark.scala
6- results folder of all csv files will be built. 
7- results were uploaded to was S3.
----- end ------



Requirements:


We are expecting you to develop a spark application to store files of the players according to their continent, please follow the following guidelines: 

1- Use the provided data sample to store a new file that contains the player name and the continent he is coming from. You are expected to use the API to do that.
2- Using parallelism is a requirement, We are expecting one file per continent as an output.  
3- Optimize your solution to expect that the data set we are providing is about a 100 GB of data.
4- After a while, we will have an updated document that has the latest salaries of the players, you are expected to build your spark application to support taking these updates and merge the new values of these players salaries into our warehouse. The new file will only have the players with the updated salaries only. 

5- Considering to store the results to an s3 bucket is preferable (create a free tier AWS account). If you couldn’t do it please provide us with zip file with the result. 
6- Output format should be csv.



Output Part:

Build hive table/s that helps you the best to answer the following questions, we are expecting you to write down the queries that answers them: 

1- Which top 3 countries that achieve the highest income through their players?

2- List The club that includes the most valuable players, The top 5 clubs that spends highest salaries 

3- Which of Europe or America - on continent level - has the best FIFA players?