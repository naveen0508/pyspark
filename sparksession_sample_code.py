from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
'''dbutils.fs.mkdirs("/test/")
dbutils.fs.put("/test/sample.txt", "naveen, 26",)'''

lines = sc.textFile("/test/sample.txt")

parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

schemaPeople = spark.createDataFrame(people)
#schemaPeople.registerTempTable("people")
schemaPeople.createOrReplaceTempView("people")

result = spark.sql("Select * from people where age < 34")

names = result.rdd.map(lambda p: "Name: " + p.name).collect()
print("Names present in the list:")
for name in names:
    print(name)

result.show()
