import pyspark
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from graphframes import GraphFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit
from pyspark.sql import SQLContext, functions as sqlfunctions, types
from pyspark.sql import functions as F
from pyspark.sql.functions import greatest
import time
spark = SparkSession.builder.appName("Project1").getOrCreate()


vertices = spark.createDataFrame([('1', 'China'), 
                                  ('2', 'South Korea'),
                                  ('3', 'Japan'),
                                  ('4', 'India'),
                                  ('5', 'Malaysia'),
                                  ('6', 'Thailand'),
                                  ('7', 'Singapore'),
                                  ('8', 'Sri Lanka'),
                                  ('9', 'Russia'),
                                  ('10', 'Finland')],
                                  ['id', 'country'])


vertices = vertices.withColumn("value",lit(-1))


edges = spark.createDataFrame([('1','2'),
                                ('2','1'),
                                ('1','3'),
                                ('3','1'),
                                ('1','4'),
                                ('4','1'),
                                ('4','5'),
                                ('5','4'),
                                ('2','3'),
                                ('3','2'),
                                ('5','6'),
                                ('6','5'),
                                ('6','7'),
                                ('7','6'),
                                ('5','8'),
                                ('8','5'),
                                ('1','9'),
                                ('9','1'),
                                ('9','10'),
                                ('10','9')],
                                ['src','dst'])


cached_vertices = AM.getCachedDataFrame(vertices)

value = -1
superstep = 0

# ### Create graph
g = GraphFrame(cached_vertices, edges)
#g.vertices.show()
#g.edges.show()
#g.degrees.show()


# ### Functions


def getid_maximum(vertex_id, msgids,i):
    return i if(int(max(msgids)) < int(vertex_id)) else -1
getid_maximum_udf = F.udf(getid_maximum, types.IntegerType())

def getid_maximum2(vertex_id, msgids,i,value):
    maxid = -1
    if(len(msgids) > 0):
        maxid = max(msgids)
    if(value != -1):
        return int(value)
    return i if(int(maxid) < int(vertex_id)) else -1
getid_maximum_udf2 = F.udf(getid_maximum2, types.IntegerType())


# ### Algorithms

i=0

def algorithm1(i,g):
    while(True):
        aggregates = g.aggregateMessages(F.collect_set(AM.msg).alias("agg"),sendToDst=F.when(AM.src['value'] == -1, AM.src["id"]))

        new_vertices = g.vertices.join(aggregates, on="id", how="left_outer").withColumn("newValue", getid_maximum_udf2("id", "agg", lit(i),"value")).drop("agg").withColumn('max_by_rows', greatest('value', 'newValue')).drop("value","newValue").withColumnRenamed("max_by_rows","value")
        cached_new_vertices = AM.getCachedDataFrame(new_vertices)
        g = GraphFrame(cached_new_vertices, g.edges)
        i += 1
        #g.vertices.show()
        g.vertices.createOrReplaceTempView("temp_table")
        if(spark.sql("SELECT * from temp_table where value = -1").count() == 0):
            final_df = g.vertices
            break
    return final_df


def algorithm2(i,g):
    while(True):
        h = g.filterVertices("value == -1")
        aggregates = h.aggregateMessages(F.collect_set(AM.msg).alias("agg"),sendToDst=AM.src["id"])
        res = aggregates.withColumn("newValue", getid_maximum_udf("id", "agg", lit(i))).drop("agg")

        new_vertices = g.vertices.join(res, on="id", how="left_outer").withColumn('max_by_rows', greatest('value', 'newValue')).drop("value","newValue").withColumnRenamed("max_by_rows","value")
        cached_new_vertices = AM.getCachedDataFrame(new_vertices)
        g = GraphFrame(cached_new_vertices, g.edges)
        i += 1
        #g.vertices.show()
        if(g.filterVertices("value == -1").dropIsolatedVertices().edges.count() == 0):
            final_df = g.vertices
            final_df = final_df.withColumn("value", F.when(final_df["value"] == -1, i).otherwise(final_df["value"]))
            break
    return final_df


#### Print final results

start_time1 = time.time()
finaldf1 = algorithm1(i,g)
elapsed1 = time.time() - start_time1

start_time2 = time.time()
finaldf2 = algorithm2(i,g)
elapsed2 = time.time() - start_time2

print("-----Algorithm1-----")
print("---Elapsed time:" + str(elapsed1))
finaldf1.show()

print("-----Algorithm2-----")
print("---Elapsed time:" + str(elapsed2))
finaldf2.show()


# for run => ~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11 /vagrant/project1_cs2190009.py