from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, lag, round, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("covid").master("local[*]").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv(
    r"c:\Users\admin\Downloads\covid-data.csv"
)
df = df.filter(col("continent").isNotNull())

day = df.filter(col("date") == "2021-03-31")
day = day.withColumn("pct", round(col("total_cases") / col("population") * 100, 2))
top15 = (
    day.filter(col("pct").isNotNull())
    .orderBy(desc("pct"))
    .limit(15)
    .select(
        col("iso_code"),
        col("location").alias("страна"),
        col("pct").alias("процент переболевших"),
    )
)

week = df.filter(
    (col("date") >= "2021-03-25")
    & (col("date") <= "2021-03-31")
    & col("new_cases").isNotNull()
)
w = Window.partitionBy("location").orderBy(desc("new_cases"), desc("date"))
top10 = (
    week.withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .orderBy(desc("new_cases"))
    .limit(10)
    .select(
        col("date").alias("число"),
        col("location").alias("страна"),
        col("new_cases").cast("int").alias("кол-во новых случаев"),
    )
)

rus = df.filter(
    (col("location") == "Russia")
    & (col("date") >= "2021-03-24")
    & (col("date") <= "2021-03-31")
)
w2 = Window.partitionBy("location").orderBy("date")
rus = rus.withColumn("prev", lag("new_cases").over(w2))
russia = rus.filter(col("date") >= "2021-03-25").select(
    col("date").alias("число"),
    col("prev").cast("int").alias("кол-во новых случаев вчера"),
    col("new_cases").cast("int").alias("кол-во новых случаев сегодня"),
    (col("new_cases") - col("prev")).cast("int").alias("дельта"),
)

def save(data, name):
    rows = data.collect()
    cols = data.columns
    with open(name, "w", encoding="utf-8") as f:
        f.write("\t".join(cols) + "\n")
        for row in rows:
            f.write("\t".join(str(row[c]) for c in cols) + "\n")
    data.show(20, truncate=False)

save(top15, "result_15.txt")
save(top10, "result_top10.txt")
save(russia, "result_russia.txt")

spark.stop()
