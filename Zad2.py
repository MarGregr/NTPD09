from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg
import os

os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .getOrCreate()

#Wczytanie danych z pliku CSV
df = spark.read.csv(
    "dane.csv",
    header=True,
    inferSchema=True
)

#Wyświetlenie danych
print("Dane:")
df.show()

#Schemat danych
print("Schemat:")
df.printSchema()

#Selekcja kolumn
print("Wybrane kolumny:")
df.select("produkt", "cena").show()

#Filtrowanie danych
print("Dane gdzie cena jest wyższa niż 1000")
df_filtered = df.filter(df.cena > 1000)
df_filtered.show()

#Grupowanie i agregacja
df.groupBy("produkt").agg(
    sum("ilosc").alias("suma_ilosci")
).show()

#Zapis do CSV
df_filtered.write.mode("overwrite").csv("output_csv")

#Zapis do Parquet
df_filtered.write.mode("overwrite").parquet("output_parquet")

print("Zapisano dane do CSV i Parquet.")
