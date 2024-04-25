from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr

# Create a Spark session
spark = SparkSession.builder.appName("VolleyballAnalysis").getOrCreate()

# Define the schema for the DataFrame
schema = "Date STRING, Team_1 STRING, Team_2 STRING, T1_Score INT, T2_Score INT, T1_Sum INT, T1_BP INT, T1_Ratio INT, T1_Srv_Sum INT, T1_Srv_Err INT, T1_Srv_Ace INT, T1_Srv_Eff STRING,
 T1_Rec_Sum INT, T1_Rec_Err INT, T1_Rec_Pos STRING, T1_Rec_Perf STRING, T1_Att_Sum INT, T1_Att_Err INT, T1_Att_Blk INT, T1_Att_Kill INT, T1_Att_Kill_Perc STRING, T1_Att_Eff STRING, T1_Blk_Sum INT,
 T1_Blk_As INT, T2_Sum INT, T2_BP INT, T2_Ratio INT, T2_Srv_Sum INT, T2_Srv_Err INT, T2_Srv_Ace INT, T2_Srv_Eff STRING, T2_Rec_Sum INT, T2_Rec_Err INT, T2_Rec_Pos STRING, T2_Rec_Perf STRING, T2_Att_Sum INT,
 T2_Att_Err INT, T2_Att_Blk INT, T2_Att_Kill INT, T2_Att_Kill_Perc STRING, T2_Att_Eff STRING, T2_Blk_Sum INT, T2_Blk_As INT, Winner INT"

# Load data from the CSV file into a DataFrame
file_path = "C:\Users\user\OneDrive\Desktop\Anaptiksi Efarmogwn kai megala Dedomena\ErgasiaEpeksergasias\Mens-Volleyball-PlusLiga-2008-2023.csv"
volleyball_df = spark.read.csv(file_path,header=True,inferSchema=True) \
    

# List of columns containing percentage values
percentage_cols = ['T1_Srv_Eff', 'T1_Rec_Pos', 'T1_Rec_Perf', 'T1_Att_Kill_Perc',
                   'T1_Att_Eff', 'T1_Att_Sum', 'T2_Srv_Eff', 'T2_Rec_Pos', 'T2_Rec_Perf', 'T2_Att_Kill_Perc',
                   'T2_Att_Eff', 'T2_Att_Sum']

# Remove percent sign from specified columns
for col_name in percentage_cols:
    volleyball_df = volleyball_df.withColumn(col_name, col(col_name).cast("double") / 100)

# Capitalize group names in 'Team_1' and 'Team_2' columns
volleyball_df = volleyball_df.withColumn("Team_1", col("Team_1"))
volleyball_df = volleyball_df.withColumn("Team_2", col("Team_2"))

# Create a new column 'Winner_Team' to represent the winning team for each match
volleyball_df = volleyball_df.withColumn("Winner_Team", when(col("Winner") == 1, col("Team_1")).otherwise(col("Team_2")))

# Calculate the number of matches played by each team (home, away, and overall)
matches_played_home = volleyball_df.groupBy("Team_1").count().withColumnRenamed("count", "Matches_Played_Home")
matches_played_away = volleyball_df.groupBy("Team_2").count().withColumnRenamed("count", "Matches_Played_Away")
matches_played_total = matches_played_home.join(matches_played_away, col("Team_1") == col("Team_2"), "full_outer") \
    .select(col("Team_1").alias("Team"), expr("coalesce(Matches_Played_Home, 0) + coalesce(Matches_Played_Away, 0)").alias("Matches_Played_Total"))

# Calculate the total sets lost and won by each team
sets_won_team_1 = volleyball_df.groupBy("Team_1").sum("T1_Score").withColumnRenamed("sum(T1_Score)", "Sets_Won")
sets_won_team_2 = volleyball_df.groupBy("Team_2").sum("T2_Score").withColumnRenamed("sum(T2_Score)", "Sets_Won")
sets_lost_team_1 = volleyball_df.groupBy("Team_1").sum("T2_Score").withColumnRenamed("sum(T2_Score)", "Sets_Lost")
sets_lost_team_2 = volleyball_df.groupBy("Team_2").sum("T1_Score").withColumnRenamed("sum(T1_Score)", "Sets_Lost")

# Calculate the total points lost and won by each team
points_won_team_1 = volleyball_df.groupBy("Team_1").sum("T1_Sum").withColumnRenamed("sum(T1_Sum)", "Points_Won")
points_won_team_2 = volleyball_df.groupBy("Team_2").sum("T2_Sum").withColumnRenamed("sum(T2_Sum)", "Points_Won")
points_lost_team_1 = volleyball_df.groupBy("Team_1").sum("T2_Sum").withColumnRenamed("sum(T2_Sum)", "Points_Lost")
points_lost_team_2 = volleyball_df.groupBy("Team_2").sum("T1_Sum").withColumnRenamed("sum(T1_Sum)", "Points_Lost")

# Combine the results into a single DataFrame
team_stats = matches_played_total 
    .join(sets_won_team_1, col("Team") == col("Team_1"), "left_outer") 
    .join(sets_won_team_2, col("Team") == col("Team_2"), "left_outer") 
    .join(sets_lost_team_1, col("Team") == col("Team_1"), "left_outer") 
    .join(sets_lost_team_2, col("Team") == col("Team_2"), "left_outer") 
    .join(points_won_team_1, col("Team") == col("Team_1"), "left_outer") 
    .join(points_won_team_2, col("Team") == col("Team_2"), "left_outer") 
    .join(points_lost_team_1, col("Team") == col("Team_1"), "left_outer") 
    .join(points_lost_team_2, col("Team") == col("Team_2"), "left_outer") 
    .select(
        "Team",
        "Matches_Played_Total",
        "Sets_Won", "Sets_Lost",
        "Points_Won", "Points_Lost"
    )

# Add a column for the total matches won in descending order
windowSpec = Window.orderBy(col("Matches_Played_Total").desc())

team_stats = team_stats.withColumn(
    'Rank', F.row_number().over(windowSpec)
)

# Save the resulting DataFrame to a CSV file
team_stats.write.csv('path/to/team_statistics', header=True, mode='overwrite')
   