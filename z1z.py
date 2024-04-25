from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, expr

# Create a Spark session
spark = SparkSession.builder.appName("VolleyballAnalysis").getOrCreate()

# Define the schema for the DataFrame
schema = "Date STRING, Team_1 STRING, Team_2 STRING, T1_Score INT, T2_Score INT, T1_Sum INT, T1_BP INT, T1_Ratio INT, T1_Srv_Sum INT, T1_Srv_Err INT, T1_Srv_Ace INT,
 T1_Srv_Eff STRING, T1_Rec_Sum INT, T1_Rec_Err INT, T1_Rec_Pos STRING, T1_Rec_Perf STRING, T1_Att_Sum INT, T1_Att_Err INT, T1_Att_Blk INT, T1_Att_Kill INT, T1_Att_Kill_Perc STRING,
 T1_Att_Eff STRING, T1_Blk_Sum INT, T1_Blk_As INT, T2_Sum INT, T2_BP INT, T2_Ratio INT, T2_Srv_Sum INT, T2_Srv_Err INT, T2_Srv_Ace INT, T2_Srv_Eff STRING, T2_Rec_Sum INT, T2_Rec_Err INT,
 T2_Rec_Pos STRING, T2_Rec_Perf STRING, T2_Att_Sum INT, T2_Att_Err INT, T2_Att_Blk INT, T2_Att_Kill INT, T2_Att_Kill_Perc STRING, T2_Att_Eff STRING, T2_Blk_Sum INT, T2_Blk_As INT, Winner INT"

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
volleyball_df = volleyball_df.withColumn("Team_1", upper(col("Team_1")))
volleyball_df = volleyball_df.withColumn("Team_2", upper(col("Team_2")))

# Calculate the number of matches played by each team
matches_by_team = volleyball_df.select("Team_1").union(volleyball_df.select("Team_2")).groupBy("Team_1").count()

# Save the result to a CSV file
output_csv_path = "path/to/output/matches_by_team.csv"
matches_by_team.write.csv(output_csv_path, header=True)

# Display the result without cropping
matches_by_team.show(truncate=False)

# Stop the Spark session
spark.stop()