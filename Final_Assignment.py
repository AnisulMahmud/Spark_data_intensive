# Databricks notebook source
# MAGIC %md
# MAGIC # Data-Intensive Programming - Assignment
# MAGIC
# MAGIC This is the **Python** version of the assignment. Switch to the Scala version, if you want to do the assignment in Scala.
# MAGIC
# MAGIC In all tasks, add your solutions to the cells following the task instructions. You are free to add new cells if you want.
# MAGIC
# MAGIC Don't forget to **submit your solutions to Moodle** once your group is finished with the assignment.
# MAGIC
# MAGIC ## Basic tasks (compulsory)
# MAGIC
# MAGIC There are in total seven basic tasks that every group must implement in order to have an accepted assignment.
# MAGIC
# MAGIC The basic task 1 is a warming up task and it deals with some video game sales data. The task asks you to do some basic aggregation operations with Spark data frames.
# MAGIC
# MAGIC The other basic tasks (basic tasks 2-7) are all related and deal with data from [https://moneypuck.com/data.htm](https://moneypuck.com/data.htm) that contains information about every shot in all National Hockey League ([NHL](https://en.wikipedia.org/wiki/National_Hockey_League), [ice hockey](https://en.wikipedia.org/wiki/Ice_hockey)) matches starting from season 2011-12 and ending with the last completed season, 2022-23. The tasks ask you to calculate the results of the matches based on the given data as well as do some further calculations. Knowledge about ice hockey or NHL is not required, and the task instructions should be sufficient in order to gain enough context for the tasks.
# MAGIC
# MAGIC ## Additional tasks (optional, can provide course points)
# MAGIC
# MAGIC There are in total of three additional tasks that can be done to gain some course points.
# MAGIC
# MAGIC The first additional task asks you to do all the basic tasks in an optimized way. It is possible that you can some points from this without directly trying by just implementing the basic tasks in an efficient manner.
# MAGIC
# MAGIC The other two additional tasks are separate tasks and do not relate to any other basic or additional tasks. One of them asks you to load in unstructured text data and do some calculations based on the words found from the data. The other asks you to utilize the K-Means algorithm to partition the given building data.
# MAGIC
# MAGIC It is possible to gain partial points from the additional tasks. I.e., if you have not completed the task fully but have implemented some part of the task, you might gain some appropriate portion of the points from the task.
# MAGIC

# COMMAND ----------

# import statements for the entire notebook


import math
from typing import List
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as Func, DataFrame
from pyspark.sql.functions import sum, col,round,dense_rank,count, expr
from pyspark.sql import functions as F
from pyspark.sql.window import Window 
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 1 - Sales data
# MAGIC
# MAGIC The CSV file `assignment/sales/video_game_sales.csv` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains video game sales data (from [https://www.kaggle.com/datasets/ashaheedq/video-games-sales-2019/data](https://www.kaggle.com/datasets/ashaheedq/video-games-sales-2019/data)). The direct address for the dataset is: `abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv`
# MAGIC
# MAGIC Load the data from the CSV file into a data frame. The column headers and the first few data lines should give sufficient information about the source dataset.
# MAGIC
# MAGIC Only data for sales in the first ten years of the 21st century should be considered in this task, i.e. years 2000-2009.
# MAGIC
# MAGIC Using the data, find answers to the following:
# MAGIC
# MAGIC - Which publisher had the highest total sales in video games in European Union in years 2000-2009?
# MAGIC - What were the total yearly sales, in European Union and globally, for this publisher in year 2000-2009
# MAGIC

# COMMAND ----------

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

#file path
file_path = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv"

#reading
video_game = spark.read.csv(file_path, header=True, inferSchema=True)

# Filtering
video_game_Year = video_game.filter((video_game['Year'] >= 2000) & (video_game['Year'] <= 2009))


# Calculate total sales in European Union for each publisher
bestEUPublisherEu = video_game_Year.groupBy('Publisher').agg(
    sum(col('EU_Sales').cast('double')).alias('EU_Total')
)

# Find the publisher with the highest total sales in European Union
bestEUPublisher:str = bestEUPublisherEu.orderBy('EU_Total', ascending=False).first()['Publisher']

#highest total sales
bestEUPublisher_match = video_game_Year.filter(video_game_Year['Publisher'] == bestEUPublisher)


# Best publisher sales by Year
bestEUPublisherSales = bestEUPublisher_match.groupBy('Year').agg( round(sum('EU_Sales'),2).alias('EU_Total'),
                                             round(sum('Global_Sales'),2).alias('Global_Total')).orderBy('Year')



print(f"The publisher with the highest total video game sales in European Union is: '{bestEUPublisher}'")
print("Sales data for the publisher:")
bestEUPublisherSales.show(10)




# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 2 - Shot data from NHL matches
# MAGIC
# MAGIC A parquet file in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/nhl_shots.parquet` from [https://moneypuck.com/data.htm](https://moneypuck.com/data.htm) contains information about every shot in all National Hockey League ([NHL](https://en.wikipedia.org/wiki/National_Hockey_League), [ice hockey](https://en.wikipedia.org/wiki/Ice_hockey)) matches starting from season 2011-12 and ending with the last completed season, 2022-23.
# MAGIC
# MAGIC In this task you should load the data with all of the rows into a data frame. This data frame object will then be used in the following basic tasks 3-7.
# MAGIC
# MAGIC ### Background information
# MAGIC
# MAGIC Each NHL season is divided into regular season and playoff season. In the regular season the teams play up to 82 games with the best teams continuing to the playoff season. During the playoff season the remaining teams are paired and each pair play best-of-seven series of games to determine which team will advance to the next phase.
# MAGIC
# MAGIC In ice hockey each game has a home team and an away team. The regular length of a game is three 20 minute periods, i.e. 60 minutes or 3600 seconds. The team that scores more goals in the regulation time is the winner of the game.
# MAGIC
# MAGIC If the scoreline is even after this regulation time:
# MAGIC
# MAGIC - In playoff games, the game will be continued until one of the teams score a goal with the scoring team being the winner.
# MAGIC - In regular season games, there is an extra time that can last a maximum of 5 minutes (300 seconds). If one of the teams score, the game ends with the scoring team being the winner. If there is no goals in the extra time, there would be a shootout competition to determine the winner. These shootout competitions are not considered in this assignment, and the shots from those are not included in the raw data.
# MAGIC
# MAGIC **Columns in the data**
# MAGIC
# MAGIC Each row in the given data represents one shot in a game.
# MAGIC
# MAGIC The column description from the source website. Not all of these will be needed in this assignment.
# MAGIC
# MAGIC | column name | column type | description |
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | shotID      | integer | Unique id for each shot |
# MAGIC | homeTeamCode | string | The home team in the game. For example: TOR, MTL, NYR, etc. |
# MAGIC | awayTeamCode | string | The away team in the game |
# MAGIC | season | integer | Season the shot took place in. Example: 2009 for the 2009-2010 season |
# MAGIC | isPlayOffGame | integer | Set to 1 if a playoff game, otherwise 0 |
# MAGIC | game_id | integer | The NHL Game_id of the game the shot took place in |
# MAGIC | time | integer | Seconds into the game of the shot |
# MAGIC | period | integer | Period of the game |
# MAGIC | team | string | The team taking the shot. HOME or AWAY |
# MAGIC | location | string | The zone the shot took place in. HOMEZONE, AWAYZONE, or Neu. Zone |
# MAGIC | event | string | Whether the shot was a shot on goal (SHOT), goal, (GOAL), or missed the net (MISS) |
# MAGIC | homeTeamGoals | integer | Home team goals before the shot took place |
# MAGIC | awayTeamGoals | integer | Away team goals before the shot took place |
# MAGIC | homeTeamWon | integer | Set to 1 if the home team won the game. Otherwise 0. |
# MAGIC | shotType | string | Type of the shot. (Slap, Wrist, etc) |
# MAGIC

# COMMAND ----------

# File path for the parquet file
file_path = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/nhl_shots.parquet"

# Load the data into a DataFrame
shotsDF: DataFrame = spark.read.parquet(file_path)


# Number of rows
num_rows = shotsDF.count()
print(f"Number of rows: {num_rows}")

# Number of columns
num_columns = len(shotsDF.columns)
print(f"Number of columns: {num_columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 3 - Game data frame
# MAGIC
# MAGIC Create a match data frame for all the game included in the shots data frame created in basic task 2.
# MAGIC
# MAGIC The output should contain one row for each game.
# MAGIC
# MAGIC The following columns should be included in the final data frame for this task:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | season         | integer     | Season the game took place in. Example: 2009 for the 2009-2010 season |
# MAGIC | game_id        | integer     | The NHL Game_id of the game |
# MAGIC | homeTeamCode   | string      | The home team in the game. For example: TOR, MTL, NYR, etc. |
# MAGIC | awayTeamCode   | string      | The away team in the game |
# MAGIC | isPlayOffGame  | integer     | Set to 1 if a playoff game, otherwise 0 |
# MAGIC | homeTeamGoals  | integer     | Number of goals scored by the home team |
# MAGIC | awayTeamGoals  | integer     | Number of goals scored by the away team |
# MAGIC | lastGoalTime   | integer     | The time in seconds for the last goal in the game. 0 if there was no goals in the game. |
# MAGIC
# MAGIC All games had at least some shots but there are some games that did not have any goals either in the regulation 60 minutes or in the extra time.
# MAGIC
# MAGIC Note, that for a couple of games there might be some shots, including goal-scoring ones, that are missing from the original dataset. For example, there might be a game with a final scoreline of 3-4 but only 6 of the goal-scoring shots are included in the dataset. Your solution does not have to try to take these rare occasions of missing data into account. I.e., you can do all the tasks with the assumption that there are no missing or invalid data.
# MAGIC

# COMMAND ----------


# Create a temporary view for the shots DataFrame
shotsDF.createOrReplaceTempView("shots")

# Define the games DataFrame using Spark SQL
gamesDF = spark.sql("""
    SELECT season, 
           game_id, 
           homeTeamCode, 
           awayTeamCode, 
           isPlayOffGame,
           SUM(CASE WHEN team = 'HOME' AND event = 'GOAL' THEN 1 ELSE 0 END) as homeTeamGoals,
           SUM(CASE WHEN team = 'AWAY' AND event = 'GOAL' THEN 1 ELSE 0 END) as awayTeamGoals,
           MAX(CASE WHEN homeTeamGoals > 0 OR awayTeamGoals > 0 THEN time ELSE 0 END) as lastGoalTime
    FROM shots
    GROUP BY season, game_id, homeTeamCode, awayTeamCode, isPlayOffGame
""")

# Cache the DataFrame
gamesDF.cache()

# Display the number of rows and show the first 5 rows
print("Number of rows:", gamesDF.count())
gamesDF.show(5)

#checking out the output for secific one
filtered_game = gamesDF.filter(
    (gamesDF['game_id'] == 20556 ) &
    (gamesDF['homeTeamCode'] == 'COL') &
    (gamesDF['awayTeamCode'] == 'DET')
)
# Display the filtered game
filtered_game.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 4 - Game wins during playoff seasons
# MAGIC
# MAGIC Create a data frame that uses the game data frame from the basic task 3 and contains aggregated number of wins and losses for each team and for each playoff season, i.e. for games which have been marked as playoff games. Only teams that have played in at least one playoff game in the considered season should be included in the final data frame.
# MAGIC
# MAGIC The following columns should be included in the final data frame:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
# MAGIC | teamCode       | string      | The code for the team. For example: TOR, MTL, NYR, etc. |
# MAGIC | games          | integer     | Number of playoff games the team played in the given season |
# MAGIC | wins           | integer     | Number of wins in playoff games the team had in the given season |
# MAGIC | losses         | integer     | Number of losses in playoff games the team had in the given season |
# MAGIC
# MAGIC Playoff games where a team scored more goals than their opponent are considered winning games. And playoff games where a team scored less goals than the opponent are considered losing games.
# MAGIC
# MAGIC In real life there should not be any playoff games where the final score line was even but due to some missing shot data you might end up with a couple of playoff games that seems to have ended up in a draw. These possible "drawn" playoff games can be left out from the win/loss calculations.
# MAGIC

# COMMAND ----------

# Filter playoff games
playoffGamesDF = gamesDF.filter(gamesDF['isPlayOffGame'] == 1)

# Calculate wins and losses for home teams
homeWinsLossesDF = playoffGamesDF.groupBy("season", "homeTeamCode").agg(
    F.sum(F.when(playoffGamesDF['homeTeamGoals'] > playoffGamesDF['awayTeamGoals'], 1).otherwise(0)).alias("wins"),
    F.sum(F.when(playoffGamesDF['homeTeamGoals'] < playoffGamesDF['awayTeamGoals'], 1).otherwise(0)).alias("losses"),
    F.count("game_id").alias("games")
).withColumnRenamed("homeTeamCode", "teamCode")

# Calculate wins and losses for away teams
awayWinsLossesDF = playoffGamesDF.groupBy("season", "awayTeamCode").agg(
    F.sum(F.when(playoffGamesDF['awayTeamGoals'] > playoffGamesDF['homeTeamGoals'], 1).otherwise(0)).alias("wins"),
    F.sum(F.when(playoffGamesDF['awayTeamGoals'] < playoffGamesDF['homeTeamGoals'], 1).otherwise(0)).alias("losses"),
    F.count("game_id").alias("games")
).withColumnRenamed("awayTeamCode", "teamCode")

# Combine home and away results
combinedDF = homeWinsLossesDF.union(awayWinsLossesDF)

# Aggregate total wins and losses for each team and season
playoffDF = combinedDF.groupBy("season", "teamCode").agg(
    F.sum("games").alias("games"),
    F.sum("wins").alias("wins"),
    F.sum("losses").alias("losses"),
    
)
# Filter out teams that did not play any playoff games
playoffDF = playoffDF.filter(playoffDF['games'] > 0)

# Display the results
playoffDF.show(10)

# Checking Filter for a specific team and season
filtered_game = playoffDF.filter(
    (playoffDF['season'] == 2021) &
    (playoffDF['teamCode'] == 'NYR')
)

filtered_game.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 5 - Best playoff teams
# MAGIC
# MAGIC Using the playoff data frame created in basic task 4 create a data frame containing the win-loss record for best playoff team, i.e. the team with the most wins, for each season. You can assume that there are no ties for the highest amount of wins in each season.
# MAGIC
# MAGIC The following columns should be included in the final data frame:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
# MAGIC | teamCode       | string      | The team code for the best performing playoff team in the given season. For example: TOR, MTL, NYR, etc. |
# MAGIC | games          | integer     | Number of playoff games the best performing playoff team played in the given season |
# MAGIC | wins           | integer     | Number of wins in playoff games the best performing playoff team had in the given season |
# MAGIC | losses         | integer     | Number of losses in playoff games the best performing playoff team had in the given season |
# MAGIC
# MAGIC Finally, fetch the details for the best playoff team in season 2022.
# MAGIC

# COMMAND ----------

windowWins = Window.partitionBy("season").orderBy(F.desc("wins"))

rankedTeams = playoffDF.withColumn("rank", F.row_number().over(windowWins))

bestPlayoffTeams: DataFrame = rankedTeams.filter( "rank==1").select(
            "season","teamCode", "games","wins","losses")

bestPlayoffTeams.show()


# COMMAND ----------

bestPlayoffTeam2022: Row = bestPlayoffTeams.filter("season == 2022").first()


bestPlayoffTeam2022Dict: dict = bestPlayoffTeam2022.asDict()
print("Best playoff team in 2022:")
print(f"    Team: {bestPlayoffTeam2022Dict.get('teamCode')}")
print(f"    Games: {bestPlayoffTeam2022Dict.get('games')}")
print(f"    Wins: {bestPlayoffTeam2022Dict.get('wins')}")
print(f"    Losses: {bestPlayoffTeam2022Dict.get('losses')}")
print("=========================================================")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 6 - Regular season points
# MAGIC
# MAGIC Create a data frame that uses the game data frame from the basic task 3 and contains aggregated data for each team and for each season for the regular season matches, i.e. the non-playoff matches.
# MAGIC
# MAGIC The following columns should be included in the final data frame:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
# MAGIC | teamCode       | string      | The code for the team. For example: TOR, MTL, NYR, etc. |
# MAGIC | games          | integer     | Number of non-playoff games the team played in the given season |
# MAGIC | wins           | integer     | Number of wins in non-playoff games the team had in the given season |
# MAGIC | losses         | integer     | Number of losses in non-playoff games the team had in the given season |
# MAGIC | goalsScored    | integer     | Total number goals scored by the team in non-playoff games in the given season |
# MAGIC | goalsConceded  | integer     | Total number goals scored against the team in non-playoff games in the given season |
# MAGIC | points         | integer     | Total number of points gathered by the team in non-playoff games in the given season |
# MAGIC
# MAGIC Points from each match are received as follows (in the context of this assignment, these do not exactly match the NHL rules):
# MAGIC
# MAGIC | points | situation |
# MAGIC | ------ | --------- |
# MAGIC | 3      | team scored more goals than the opponent during the regular 60 minutes |
# MAGIC | 2      | the score line was even after 60 minutes but the team scored a winning goal during the extra time |
# MAGIC | 1      | the score line was even after 60 minutes but the opponent scored a winning goal during the extra time or there were no goals in the extra time |
# MAGIC | 0      | the opponent scored more goals than the team during the regular 60 minutes |
# MAGIC
# MAGIC In the regular season the following table shows how wins and losses should be considered (in the context of this assignment):
# MAGIC
# MAGIC | win | loss | situation |
# MAGIC | --- | ---- | --------- |
# MAGIC | Yes | No   | team gained at least 2 points from the match |
# MAGIC | No  | Yes  | team gain at most 1 point from the match |
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum

home_regularSeasonDF = gamesDF.filter(col("isPlayOffGame") == 0) \
    .withColumn("points",
                when(col("lastGoalTime") == 0, 1).
                when((col("lastGoalTime") <= 3600) & (col("homeTeamGoals") < col("awayTeamGoals")), 0).
                when((col("lastGoalTime") <= 3600) & (col("homeTeamGoals") > col("awayTeamGoals")), 3).
                when((col("lastGoalTime") <= 3600) & (col("homeTeamGoals") == col("awayTeamGoals")), 1).
                when((col("lastGoalTime") > 3600) & (col("homeTeamGoals") <= col("awayTeamGoals")), 1).
                when((col("lastGoalTime") > 3600) & (col("homeTeamGoals") > col("awayTeamGoals")), 2)) \
    .withColumn("loss", when(col("points") < 2, 1).otherwise(0)) \
    .withColumn("win", when(col("points") >= 2, 1).otherwise(0)) \
    .groupBy("season", "homeTeamCode") \
    .agg(count("game_id").alias("games"),
         sum("win").alias("wins"),
         sum("loss").alias("losses"),
         sum("points").alias("points"),
         sum("homeTeamGoals").alias("goalsScored"),
         sum("awayTeamGoals").alias("goalsConceded")) \
    .withColumnRenamed("homeTeamCode", "teamCode")

away_regularSeasonDF = gamesDF.filter(col("isPlayOffGame") == 0) \
    .withColumn("points",
                when(col("lastGoalTime") == 0, 1).
                when((col("lastGoalTime") <= 3600) & (col("awayTeamGoals") < col("homeTeamGoals")), 0).
                when((col("lastGoalTime") <= 3600) & (col("awayTeamGoals") > col("homeTeamGoals")), 3).
                when((col("lastGoalTime") <= 3600) & (col("homeTeamGoals") == col("awayTeamGoals")), 1).
                when((col("lastGoalTime") > 3600) & (col("awayTeamGoals") <= col("homeTeamGoals")), 1).
                when((col("lastGoalTime") > 3600) & (col("awayTeamGoals") > col("homeTeamGoals")), 2)) \
    .withColumn("loss", when(col("points") < 2, 1).otherwise(0)) \
    .withColumn("win", when(col("points") >= 2, 1).otherwise(0)) \
    .groupBy("season", "awayTeamCode") \
    .agg(count("game_id").alias("games"),
         sum("win").alias("wins"),
         sum("loss").alias("losses"),
         sum("points").alias("points"),
         sum("awayTeamGoals").alias("goalsScored"),
         sum("homeTeamGoals").alias("goalsConceded")) \
    .withColumnRenamed("awayTeamCode", "teamCode")

regularSeasonDF = home_regularSeasonDF.union(away_regularSeasonDF) \
    .groupBy("season", "teamCode") \
    .agg(sum("games").alias("games"),
         sum("wins").alias("wins"),
         sum("losses").alias("losses"),
         sum("goalsScored").alias("goalsScored"),
         sum("goalsConceded").alias("goalsConceded"),
         sum("points").alias("points"),)

regularSeasonDF.show(5)

#for checking output for specific one
outputDF = regularSeasonDF.filter(
    (regularSeasonDF['season'] == 2022) &
    (regularSeasonDF['teamCode'] == 'CGY') )

outputDF.show()
num_rows = regularSeasonDF.count()
print(f"Number of rows: {num_rows}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 7 - The worst regular season teams
# MAGIC
# MAGIC Using the regular season data frame created in the basic task 6, create a data frame containing the regular season records for the worst regular season team, i.e. the team with the least amount of points, for each season. You can assume that there are no ties for the lowest amount of points in each season.
# MAGIC
# MAGIC Finally, fetch the details for the worst regular season team in season 2022.
# MAGIC

# COMMAND ----------

windowDeptDF = Window.partitionBy("season").orderBy(col("points"))
worstRegularTeams = regularSeasonDF.withColumn("rank", dense_rank().over(windowDeptDF)) \
    .where(col("rank") == 1).drop("rank")

worstRegularTeams.show()


# COMMAND ----------

worstRegularTeam2022: Row = worstRegularTeams.filter(col("season") == 2022).take(1)[0]

worstRegularTeam2022Dict: dict = worstRegularTeam2022.asDict()
print("Worst regular season team in 2022:")
print(f"    Team: {worstRegularTeam2022Dict.get('teamCode')}")
print(f"    Games: {worstRegularTeam2022Dict.get('games')}")
print(f"    Wins: {worstRegularTeam2022Dict.get('wins')}")
print(f"    Losses: {worstRegularTeam2022Dict.get('losses')}")
print(f"    Goals scored: {worstRegularTeam2022Dict.get('goalsScored')}")
print(f"    Goals conceded: {worstRegularTeam2022Dict.get('goalsConceded')}")
print(f"    Points: {worstRegularTeam2022Dict.get('points')}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional tasks
# MAGIC
# MAGIC The implementation of the basic tasks is compulsory for every group.
# MAGIC
# MAGIC Doing the following additional tasks you can gain course points which can help in getting a better grade from the course (or passing the course).
# MAGIC Partial solutions can give partial points.
# MAGIC
# MAGIC The additional task 1 will be considered in the grading for every group based on their solutions for the basic tasks.
# MAGIC
# MAGIC The additional tasks 2 and 3 are separate tasks that do not relate to any other task in the assignment. The solutions used in these other additional tasks do not affect the grading of additional task 1. Instead, a good use of optimized methods can positively affect the grading of each specific task, while very non-optimized solutions can have a negative effect on the task grade.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Task 1 - Optimized solutions to the basic tasks (2 points)
# MAGIC
# MAGIC Use the tools Spark offers effectively and avoid unnecessary operations in the code for the basic tasks.
# MAGIC
# MAGIC A couple of things to consider (**NOT** even close to a complete list):
# MAGIC
# MAGIC - Consider using explicit schemas when dealing with CSV data sources.
# MAGIC - Consider only including those columns from a data source that are actually needed.
# MAGIC - Filter unnecessary rows whenever possible to get smaller datasets.
# MAGIC - Avoid collect or similar expensive operations for large datasets.
# MAGIC - Consider using explicit caching if some data frame is used repeatedly.
# MAGIC - Avoid unnecessary shuffling (for example sorting) operations.
# MAGIC
# MAGIC It is okay to have your own test code that would fall into category of "ineffective usage" or "unnecessary operations" while doing the assignment tasks. However, for the final Moodle submission you should comment out or delete such code (and test that you have not broken anything when doing the final modifications).
# MAGIC
# MAGIC Note, that you should not do the basic tasks again for this additional task, but instead modify your basic task code with more efficient versions.
# MAGIC
# MAGIC You can create a text cell below this one and describe what optimizations you have done. This might help the grader to better recognize how skilled your work with the basic tasks has been.
# MAGIC

# COMMAND ----------

# Basic task 1


# Define the schema
schema = StructType([
    StructField("Rank", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Platform", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Genre", StringType(), True),
    StructField("Publisher", StringType(), True),
    StructField("NA_Sales", DoubleType(), True),
    StructField("EU_Sales", DoubleType(), True),
    StructField("JP_Sales", DoubleType(), True),
    StructField("Other_Sales", DoubleType(), True),
    StructField("Global_Sales", DoubleType(), True),
])

# Initialize Spark session
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# File path
file_path = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv"

# Reading with specific schema
video_game = spark.read.schema(schema).csv(file_path, header=True)

# Filtering for the specified year range
video_game_Year = video_game.filter((video_game['Year'] >= 2000) & (video_game['Year'] <= 2009))

# Calculate total sales in European Union for each publisher
bestEUPublisherEu = video_game_Year.groupBy('Publisher').agg(
    sum(col('EU_Sales')).alias('EU_Total')
)

# Find the publisher with the highest total sales in European Union
bestEUPublisher = bestEUPublisherEu.orderBy('EU_Total', ascending=False).first()['Publisher']

# Filter for the best European Union publisher
bestEUPublisher_match = video_game_Year.filter(video_game_Year['Publisher'] == bestEUPublisher)

# Best publisher sales by Year
bestEUPublisherSales = bestEUPublisher_match.groupBy('Year').agg(
    round(sum('EU_Sales'), 2).alias('EU_Total'),
    round(sum('Global_Sales'), 2).alias('Global_Total')
).orderBy('Year')

print(f"The publisher with the highest total video game sales in European Union is: '{bestEUPublisher}'")
print("Sales data for the publisher:")
bestEUPublisherSales.show(10)

# COMMAND ----------

# Basic Task 2


# Define the schema
schema = StructType([
    StructField("shotID", IntegerType(), True),
    StructField("homeTeamCode", StringType(), True),
    StructField("awayTeamCode", StringType(), True),
    StructField("season", IntegerType(), True),
    StructField("isPlayOffGame", IntegerType(), True),
    StructField("game_id", IntegerType(), True),
    StructField("time", IntegerType(), True),
    StructField("period", IntegerType(), True),
    StructField("team", StringType(), True),
    StructField("location", StringType(), True),
    StructField("event", StringType(), True),
    StructField("homeTeamGoals", IntegerType(), True),
    StructField("awayTeamGoals", IntegerType(), True),
    StructField("homeTeamWon", IntegerType(), True),
    StructField("shotType", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder.appName("NHLShots").getOrCreate()

# Read the parquet file with the specified schema
shotsDF = spark.read.schema(schema).parquet("abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/nhl_shots.parquet") \
    .select("season", "game_id", "homeTeamCode", "awayTeamCode", "isPlayOffGame", "homeTeamGoals", 
            "awayTeamGoals", "time", "event", "team")

# Cache the DataFrame
shotsDF.cache()

# Display the number of rows and show the first 5 rows
print("Number of rows:", shotsDF.count())
shotsDF.show(5)



# COMMAND ----------

# Basic Task 3
file_path = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/nhl_shots.parquet"
# Read data into shotsDF DataFrame

shotsDF = spark.read.schema(schema).parquet(file_path)

# Create a temporary view for the shots DataFrame
shotsDF.createOrReplaceTempView("shots")

# Define the games DataFrame using Spark SQL
gamesDF = shotsDF.groupBy(
    "season", "game_id", "homeTeamCode", "awayTeamCode", "isPlayOffGame"
).agg(
    F.sum(F.when((F.col("team") == 'HOME') & (F.col("event") == 'GOAL'), 1).otherwise(0)).alias("homeTeamGoals"),
    F.sum(F.when((F.col("team") == 'AWAY') & (F.col("event") == 'GOAL'), 1).otherwise(0)).alias("awayTeamGoals"),
    F.max(F.when((F.col("homeTeamGoals") > 0) | (F.col("awayTeamGoals") > 0), F.col("time"))).alias("lastGoalTime")
)

# Cache the DataFrame
gamesDF.cache()

# Display the number of rows and show the first 5 rows
print("Number of rows:", gamesDF.count())
gamesDF.show(5)

# Checking out the output for a specific game
filtered_game = gamesDF.filter(
    (gamesDF['game_id'] == 20556) &
    (gamesDF['homeTeamCode'] == 'COL') &
    (gamesDF['awayTeamCode'] == 'DET')
)

# Display the filtered game
filtered_game.show()

# COMMAND ----------

# Basic Task 4


# Filter playoff games
playoffGamesDF = gamesDF.filter(col("isPlayOffGame") == 1)

# Define win and loss columns for home and away teams
winsLossesDF = playoffGamesDF.withColumn("homeWin", when(col("homeTeamGoals") > col("awayTeamGoals"), 1).otherwise(0)) \
    .withColumn("homeLoss", when(col("homeTeamGoals") < col("awayTeamGoals"), 1).otherwise(0)) \
    .withColumn("awayWin", when(col("awayTeamGoals") > col("homeTeamGoals"), 1).otherwise(0)) \
    .withColumn("awayLoss", when(col("awayTeamGoals") < col("homeTeamGoals"), 1).otherwise(0))

# Aggregate wins and losses for home teams
homeWinsLossesDF = winsLossesDF.groupBy("season", "homeTeamCode") \
    .agg(
        sum("homeWin").alias("wins"),
        sum("homeLoss").alias("losses"),
        count("game_id").alias("games")
    ) \
    .withColumnRenamed("homeTeamCode", "teamCode")

# Aggregate wins and losses for away teams
awayWinsLossesDF = winsLossesDF.groupBy("season", "awayTeamCode") \
    .agg(
        sum("awayWin").alias("wins"),
        sum("awayLoss").alias("losses"),
        count("game_id").alias("games")
    ) \
    .withColumnRenamed("awayTeamCode", "teamCode")

# Union home and away results, then aggregate for each team and season
playoffDF = homeWinsLossesDF.union(awayWinsLossesDF) \
    .groupBy("season", "teamCode") \
    .agg(
        sum("games").alias("games"),
        sum("wins").alias("wins"),
        sum("losses").alias("losses")
    )

playoffDF.show(10)

filtered_game = playoffDF.filter(
    (playoffDF['season'] == 2017) &
    (playoffDF['teamCode'] == 'BOS') )
    

filtered_game.show()

# COMMAND ----------

# Basic Task 5
relevantPlayoffDF = playoffDF.select("season", "teamCode", "games", "wins", "losses")

# Define a window specification
windowSpec = Window().partitionBy("season").orderBy(F.col("wins").desc())

# Use row_number() over the window to rank teams
bestPlayoffTeams = relevantPlayoffDF.withColumn("row", F.row_number().over(windowSpec)) \
    .filter(F.col("row") == 1) \
    .drop("row")

bestPlayoffTeams.show()
bestPlayoffTeam2022: Row = bestPlayoffTeams.filter("season == 2022").first()
print('Best Team of 2022')
print(bestPlayoffTeam2022)

# COMMAND ----------

# Basic Task 6


nonPlayOffDF = gamesDF.filter(col("isPlayOffGame") == 0)

teamGoalsDF = nonPlayOffDF.select(
    "season", nonPlayOffDF["homeTeamCode"].alias("teamCode"), nonPlayOffDF["homeTeamGoals"].alias("goalsScored"), nonPlayOffDF["awayTeamGoals"].alias("goalsConceded"), "lastGoalTime"
).union(
    nonPlayOffDF.select(
        "season", nonPlayOffDF["awayTeamCode"].alias("teamCode"), nonPlayOffDF["awayTeamGoals"].alias("goalsScored"), nonPlayOffDF["homeTeamGoals"].alias("goalsConceded"), "lastGoalTime"
    )
)

teamGoalsDF = teamGoalsDF.withColumn(
    "points",
    when((col("goalsScored") > col("goalsConceded")) & (col("lastGoalTime") <= 3600), 3)
    .when(((col("goalsScored") < col("goalsConceded")) & (col("lastGoalTime") > 3600)), 1)
    .when((col("goalsScored") == col("goalsConceded")), 1)
    .when((col("goalsScored") > col("goalsConceded")) & (col("lastGoalTime") > 3600), 2)
    .otherwise(0)
)

teamResultsDF = teamGoalsDF.withColumn(
    "wins", when(col("points") >= 2, 1).otherwise(0)
).withColumn( 
    "losses", when((col("points") <= 1), 1).otherwise(0)
)

regularSeasonDF = teamResultsDF.groupBy("season", "teamCode").agg(
    expr("count(*) as games"),
    expr("sum(wins) as wins"),
    expr("sum(losses) as losses"),
    expr("sum(goalsScored) as goalsScored"),
    expr("sum(goalsConceded) as goalsConceded"),
    expr("sum(points) as points")
)

regularSeasonDF.show(5)

XDF = regularSeasonDF.filter(
    (regularSeasonDF['season'] == 2022) &
    (regularSeasonDF['teamCode'] == 'NYI') )

XDF.show()


# COMMAND ----------

# Basic Tassk 7



worstRegularTeams = (
    regularSeasonDF
    .withColumn("rank", dense_rank().over(Window.partitionBy("season").orderBy("points")))
    .filter(col("rank") == 1)
    .drop("rank")
)
# Show the worst regular season teams for each season
worstRegularTeams.show()
# Fetch details for the worst regular season team in 2022
worstRegularTeams2022 = worstRegularTeams.filter("season == 2022")

# Show the worst regular season for 2022
worstRegularTeams2022.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Task 2 - Unstructured data (2 points)
# MAGIC
# MAGIC You are given some text files with contents from a few thousand random articles both in English and Finnish from Wikipedia. Content from English articles are in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/wikipedia/en` and content from Finnish articles are at folder `assignment/wikipedia/fi`.
# MAGIC
# MAGIC Some cleaning operations have already been done to the texts but the some further cleaning is still required.
# MAGIC
# MAGIC The final goal of the task is to get the answers to following questions:
# MAGIC
# MAGIC - What are the ten most common English words that appear in the English articles?
# MAGIC - What are the five most common 5-letter Finnish words that appear in the Finnish articles?
# MAGIC - What is the longest word that appears at least 150 times in the articles?
# MAGIC - What is the average English word length for the words appearing in the English articles?
# MAGIC - What is the average Finnish word length for the words appearing in the Finnish articles?
# MAGIC
# MAGIC For a word to be included in the calculations, it should fulfill the following requirements:
# MAGIC
# MAGIC - Capitalization is to be ignored. I.e., words "English", "ENGLISH", and "english" are all to be considered as the same word "english".
# MAGIC - An English word should only contain the 26 letters from the alphabet of Modern English. Only exception is that punctuation marks, i.e. hyphens `-`, are allowed in the middle of the words as long as there are no two consecutive punctuation marks without any letters between them.
# MAGIC - The only allowed 1-letter English words are `a` and `i`.
# MAGIC - A Finnish word should follow the same rules as English words, except that three additional letters, `å`, `ä`, and `ö`, are also allowed, and that no 1-letter words are allowed. Also, any word that contains "`wiki`" should not be considered as Finnish word.
# MAGIC
# MAGIC Some hints:
# MAGIC
# MAGIC - Using an RDD or a Dataset (in Scala) might make the data cleaning and word determination easier than using DataFrames.
# MAGIC - It can be assumed that in the source data each word in the same line is separated by at least one white space (` `).
# MAGIC - You are allowed to remove all non-allowed characters from the source data at the beginning of the cleaning process.
# MAGIC - It is advisable to first create a DataFrame/Dataset/RDD that contains the found words, their language, and the number of times those words appeared in the articles. This can then be used as the starting point when determining the answers to the given questions.
# MAGIC

# COMMAND ----------


from pyspark.sql.functions import col, udf, explode, split, length, regexp_replace
from pyspark.sql.types import StringType
from typing import List

# Create a Spark session
spark = SparkSession.builder.appName("WikipediaAnalysis").getOrCreate()

# Constants
allowedOneLetterWords: List[str] = ["a", "i"]
englishLetters: str = "abcdefghijklmnopqrstuvwxyz"
finnishLetters: str = englishLetters + "åäö"
whiteSpace: str = " "
punctuationMark: str = '-'
allowedEnglishOneLetterWords: List[str] = ["a", "i"]
wikiStr: str = "wiki"

# Sample paths
english_path = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/wikipedia/en/*.txt"
finnish_path = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/wikipedia/fi/*.txt"

# Read text files into RDDs
english_rdd = spark.sparkContext.textFile(english_path)
finnish_rdd = spark.sparkContext.textFile(finnish_path)

# Function to clean and filter words
def clean_and_filter_words(text: str, allowed_letters: set, one_letter_words: List[str]) -> list:
    cleaned_text = ''.join(char.lower() if char.lower() in allowed_letters or char == ' ' else ' ' for char in text)
    words = cleaned_text.split()
    translator = str.maketrans("", "", f"{whiteSpace}{punctuationMark}")
    filtered_words = [word.strip().translate(translator) for word in words if len(word) > 1 and word not in one_letter_words]
    return filtered_words

# Register UDFs
clean_and_filter_english_udf = udf(lambda x: clean_and_filter_words(x, set(englishLetters), allowedEnglishOneLetterWords), StringType())
clean_and_filter_finnish_udf = udf(lambda x: clean_and_filter_words(x, set(finnishLetters), []), StringType())

# Apply UDFs to RDDs and create DataFrames
commonWordsEn = (
    english_rdd
    .map(lambda x: (x,))
    .toDF(["text"])
    .withColumn("words", explode(split(clean_and_filter_english_udf("text"), " ")))
    .withColumn("words", regexp_replace(col("words"), ",", ""))
    .withColumn("words", regexp_replace(col("words"), "[\[\]]", ""))
    .groupBy("words")
    .count()
    .filter((col("words").isin(allowedEnglishOneLetterWords) | (length(col("words")) > 1)) & (col("words") != ""))
    .orderBy(col("count").desc())
    .limit(10)
)

# Show results
print("The ten most common English words that appear in the English articles:")
commonWordsEn.show(truncate=False)

# COMMAND ----------

common5LetterWordsFi = (
    finnish_rdd
    .map(lambda x: (x,))
    .toDF(["text"])
    .withColumn("words", explode(split(clean_and_filter_finnish_udf("text"), " ")))
    .withColumn("words", regexp_replace(col("words"), ",", ""))
    .withColumn("words", regexp_replace(col("words"), "[\[\]]", ""))
    .groupBy("words")
    .count()
    .filter((col("words").isin(allowedOneLetterWords) | (length(col("words")) == 5)) & (col("words") != ""))
    .orderBy(col("count").desc())
    .limit(5)
)


print("The five most common 5-letter Finnish words that appear in the Finnish articles:")
common5LetterWordsFi.show()

# COMMAND ----------


allWordsEn = (
    english_rdd
    .map(lambda x: (x,))
    .toDF(["text"])
    .withColumn("words", explode(split(clean_and_filter_english_udf("text"), " ")))
    .withColumn("words", regexp_replace(col("words"), ",", ""))
    .withColumn("words", regexp_replace(col("words"), "[\[\]]", ""))
    .filter(length(col("words")) > 0)  # Filter out empty strings
    .select("words")
)

allWordsFi = (
    finnish_rdd
    .map(lambda x: (x,))
    .toDF(["text"])
    .withColumn("words", explode(split(clean_and_filter_finnish_udf("text"), " ")))
    .withColumn("words", regexp_replace(col("words"), ",", ""))
    .withColumn("words", regexp_replace(col("words"), "[\[\]]", ""))
    .filter(length(col("words")) > 0)  # Filter out empty strings
    .select("words")
)

# Combine English and Finnish words
combinedWords = allWordsEn.union(allWordsFi)

# Find the longest word appearing at least 150 times
result = (
    combinedWords
    .groupBy("words")
    .count()
    .filter(col("count") >= 150)
    .orderBy(length(col("words")).desc())
    .limit(1)
)

# Extract the word from the DataFrame
longest_word_row = result.select("words").first()
longest_word = longest_word_row["words"]
print(f"The longest word appearing at least 150 times is '{longest_word}'")

# COMMAND ----------

from pyspark.sql.functions import col, length, avg

# Calculate average word lengths for English
average_word_lengths_english = (
    allWordsEn
    .groupBy()
    .agg(avg(length(col("words"))).alias("average_length_english"))
)

# Calculate average word lengths for Finnish
average_word_lengths_finnish = (
    allWordsFi
    .groupBy()
    .agg(avg(length(col("words"))).alias("average_length_finnish"))
)

# Create a new DataFrame with the information
result_data = [
    ("Finnish", "{:.2f}".format(average_word_lengths_finnish.first()["average_length_finnish"])),
    ("English", "{:.2f}".format(average_word_lengths_english.first()["average_length_english"]))
]

result_columns = ["Language", "Average_Length"]

averageWordLengths = spark.createDataFrame(result_data, result_columns)

print("The average word lengths:")
averageWordLengths.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Task 3 - K-Means clustering (2 points)
# MAGIC
# MAGIC You are given a dataset containing the locations of building in Finland. The dataset is a subset from [https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f](https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f) limited to only postal codes with the first two numbers in the interval 30-44 ([postal codes in Finland](https://www.posti.fi/en/zip-code-search/postal-codes-in-finland)). The dataset is in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/buildings.parquet`.
# MAGIC
# MAGIC [K-Means clustering](https://en.wikipedia.org/wiki/K-means_clustering) algorithm is an unsupervised machine learning algorithm that can be used to partition the input data into k clusters. Your task is to use the Spark ML library and its K-Means clusterization algorithm to divide the buildings into clusters using the building coordinates `latitude_wgs84` and `longitude_wgs84` as the basis of the clusterization. You should implement the following procedure:
# MAGIC
# MAGIC 1. Start with all the buildings in the dataset.
# MAGIC 2. Divide the buildings into seven clusters with K-Means algorithm using `k=7` and the longitude and latitude of the buildings.
# MAGIC 3. Find the cluster to which the Sähkötalo building from the Hervanta campus is sorted into. The building id for Sähkötalo in the dataset is `102363858X`.
# MAGIC 4. Choose all the buildings from the cluster with the Sähkötalo building.
# MAGIC 5. Find the cluster center for the chosen cluster of buildings.
# MAGIC 6. Calculate the largest distance from a building in the chosen cluster to the chosen cluster center. You are given a function `haversine` that you can use to calculate the distance between two points using the latitude and longitude of the points.
# MAGIC 7. While the largest distance from a building in the considered cluster to the cluster center is larger than 3 kilometers run the K-Means algorithm again using the following substeps.
# MAGIC     - Run the K-Means algorithm to divide the remaining buildings into smaller clusters. The number of the new clusters should be one less than in the previous run of the algorithm (but should always be at least two). I.e., the sequence of `k` values starting from the second run should be 6, 5, 4, 3, 2, 2, ...
# MAGIC     - After using the algorithm again, choose the new smaller cluster of buildings so that it includes the Sähkötalo building.
# MAGIC     - Find the center of this cluster and calculate the largest distance from a building in this cluster to its center.
# MAGIC
# MAGIC As the result of this process, you should get a cluster of buildings that includes the Sähkötalo building and in which all buildings are within 3 kilometers of the cluster center.
# MAGIC
# MAGIC Using the final cluster, find the answers to the following questions:
# MAGIC
# MAGIC - How many buildings in total are in the final cluster?
# MAGIC - How many Hervanta buildings are in this final cluster? (A building is considered to be in Hervanta if their postal code is `33720`)
# MAGIC
# MAGIC Some hints:
# MAGIC
# MAGIC - Once you have trained a KMeansModel, the coordinates for the cluster centers, and the cluster indexes for individual buildings can be accessed through the model object (`clusterCenters`, `summary.predictions`).
# MAGIC - The given haversine function for calculating distances can be used with data frames if you turn it into an user defined function.
# MAGIC

# COMMAND ----------

startK: int = 7
seedValue: int = 1

# the building id for Sähkötalo building at Hervanta campus
hervantaBuildingId: str = "102363858X"
hervantaPostalCode: int = 33720

maxAllowedClusterDistance: float = 3.0


# returns the distance between points (lat1, lon1) and (lat2, lon2) in kilometers
# based on https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R: float = 6378.1  # radius of Earth in kilometers
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    deltaPhi = math.radians(lat2 - lat1)
    deltaLambda = math.radians(lon2 - lon1)

    a = (
        math.sin(deltaPhi * deltaPhi / 4.0) +
        math.cos(phi1) * math.cos(phi2) * math.sin(deltaLambda * deltaLambda / 4.0)
    )

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
import math

# Initialize Spark session
spark = SparkSession.builder.appName("BuildingClustering").getOrCreate()

# Load the dataset
file_path = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/buildings.parquet"
buildings_df = spark.read.parquet(file_path)

# Define the Haversine function
def haversine(lat1, lon1, lat2, lon2):
    R = 6378.1  # radius of Earth in kilometers
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    deltaPhi = math.radians(lat2 - lat1)
    deltaLambda = math.radians(lon2 - lon1)

    a = (
        math.sin(deltaPhi * deltaPhi / 4.0) +
        math.cos(phi1) * math.cos(phi2) * math.sin(deltaLambda * deltaLambda / 4.0)
    )

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# Define Haversine as a user-defined function
@udf(FloatType())
def haversine_udf(lat, lon):
    return haversine(lat, lon, cluster_center[0], cluster_center[1])

# some helpful constants
startK: int = 7
seedValue: int = 1

# the building id for Sähkötalo building at Hervanta campus
hervantaBuildingId: str = "102363858X"
hervantaPostalCode: int = 33720

maxAllowedClusterDistance: float = 3.0

# Create a VectorAssembler to assemble the features
vec_assembler = VectorAssembler(inputCols=["latitude_wgs84", "longitude_wgs84"], outputCol="features")
buildings_df = vec_assembler.transform(buildings_df)

# Initialize variables
k = startK
iteration = 1

# Placeholder for the previous cluster center and selected cluster df
prev_cluster_center = None
prev_selected_cluster_df = None

while True:
    # Use K-Means algorithm
    kmeans = KMeans(featuresCol="features", k=k, seed=seedValue)
    model = kmeans.fit(buildings_df)

    # Use previous values for the first iteration
    if prev_cluster_center is None:
        prev_cluster_center = model.clusterCenters()[0]
        prev_selected_cluster_df = buildings_df

    predictions_col = f"prediction_{iteration}"
    predictions = model.transform(buildings_df).withColumnRenamed("prediction", predictions_col)

    # Find the cluster to which the Sähkötalo building from the Hervanta campus is sorted into
    sahkotalo_cluster = predictions.filter(col("building_id") == hervantaBuildingId).select(predictions_col).first()[predictions_col]

    # Choose all the buildings from the cluster with the Sähkötalo building
    selected_cluster_df = predictions.filter(col(predictions_col) == sahkotalo_cluster)

    # Find the center of this cluster
    cluster_center = model.clusterCenters()[sahkotalo_cluster]
    max_distance = (
    selected_cluster_df
    .withColumn("distance", haversine_udf(col("latitude_wgs84"), col("longitude_wgs84")))
    .groupBy(predictions_col)
    .agg({"distance": "max"})
    .first()["max(distance)"]
)




    # Display the cluster information for each iteration
    print(f"(k={k}, iteration={iteration}) Buildings: {prev_selected_cluster_df.count()} -> {selected_cluster_df.count()}, Maximum distance to the center within 'Sähkötalo' cluster: {max_distance:.2f} km")

    # Check if the condition is met
    if max_distance <= maxAllowedClusterDistance or k == 2:
        print(f"Exiting due to negligible distance reduction.")
        break

    # Update variables for the next iteration
    k -= 1
    iteration += 1
    prev_max_distance = max_distance
    prev_cluster_center = cluster_center
    prev_selected_cluster_df = selected_cluster_df
    buildings_df = selected_cluster_df  # Update the dataset for the next iteration

# Display the final cluster of buildings
finalCluster = selected_cluster_df

# Calculate counts
clusterBuildingCount: int = finalCluster.count()
clusterHervantaBuildingCount: int = finalCluster.filter(col("postal_code") == hervantaPostalCode).count()


# Print the results
print(f"Buildings in the final cluster: {clusterBuildingCount}")
print(f"Hervanta buildings in the final cluster: {clusterHervantaBuildingCount} ", end="")
print(f"({round(100*clusterHervantaBuildingCount/clusterBuildingCount, 2)}% of all buildings in the final cluster)")
print("===========================================================================================")
