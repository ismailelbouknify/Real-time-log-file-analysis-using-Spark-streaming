from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
from pyspark import SparkContext

# Loads data.
#dataset = spark.read.format("mongo").load("data/mllib/sample_kmeans_data.txt")
dataset=pd.read_parquet(r'C:\Users\hp\BDSaS\Big_Data\logs_df.parquet')
# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
 
df = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/people.contacts").load()

from pyspark.sql import SparkSession
my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mydb.persons") \
    .getOrCreate()