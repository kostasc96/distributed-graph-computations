#!/usr/bin/env python
# coding: utf-8

# In[159]:
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
spark = SparkSession.builder.appName("Project1").getOrCreate()


vertices = spark.createDataFrame([('1', 'China'), 
                                  ('2', 'South Korea'),
                                  ('3', 'Japan'),
                                  ('4', 'India'),
                                  ('5', 'Malaysia'),
                                  ('6', 'Thailand'),
                                  ('7', 'Singapore'),
                                  ('8', 'Sri Lanka')],
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
                                ('8','5')],
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
    return i if(max(msgids) < vertex_id) else -1
getid_maximum_udf = F.udf(getid_maximum, types.IntegerType())


# ### Algorithm


max_iterations = 5
i=0
global final_df
while(True):
    h = g.filterVertices("value == -1")
    aggregates = h.aggregateMessages(F.collect_set(AM.msg).alias("agg"),sendToDst=AM.src["id"])
    res = aggregates.withColumn("newValue", getid_maximum_udf("id", "agg", lit(i))).drop("agg")

    new_vertices = g.vertices.join(res, on="id", how="left_outer").withColumn('max_by_rows', greatest('value', 'newValue')).drop("value","newValue").withColumnRenamed("max_by_rows","value")
    cached_new_vertices = AM.getCachedDataFrame(new_vertices)
    g = GraphFrame(cached_new_vertices, g.edges)
    i += 1
    g2 = g.filterVertices("value == -1").dropIsolatedVertices()
    if(g2.edges.count() == 0):
        final_df = g.vertices
        final_df = final_df.withColumn("value", F.when(final_df["value"] == -1, i).otherwise(final_df["value"]))
        break
    #g.vertices.createOrReplaceTempView("temp_table")
    #if(spark.sql("SELECT * from temp_table where value = -1").count() == 1):
    #    final_df = g.vertices
    #    break

final_df.show()


# ### Print result
#cached_new_vertices.printSchema()



# for run => ~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11 /vagrant/project1_cs2190009.py