from pyspark.sql import SparkSession
import os
import sys

os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("RDDExample") \
    .getOrCreate()

sc = spark.sparkContext

#Wczytanie pliku jako RDD
rdd = sc.textFile("dane.csv")

#Usunięcie nagłówka
header = rdd.first()
data = rdd.filter(lambda row: row != header)

#Parsowanie danych
parsed = data.map(lambda line: line.split(","))

#Wyświetlenie danych
print("Dane:")
for row in parsed.collect():
    print(row)

#Filtrowanie danych - przedmioty, których cena (indeks 2) jest większa niż 1000
filtered = parsed.filter(lambda x: int(x[2]) > 1000)

print("\nProdukty z ceną > 1000:")
for row in filtered.collect():
    print(row)

#Obliczenie łącznej wartości sprzedaży
total_sales = parsed.map(
    lambda x: int(x[2]) * int(x[3])
).reduce(lambda a, b: a + b)

print("\nŁączna wartość sprzedaży:", total_sales)

#Liczba rekordów
count = parsed.count()
print("Liczba rekordów danych:", count)

#Zamykanie kontekstu Sparka po zakończeniu pracy
spark.stop()
