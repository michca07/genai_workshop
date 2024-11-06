# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable CDC for Vector Search Delta Sync
# MAGIC ALTER TABLE shm.default.chunked_docs 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

from pyspark.sql import functions as F

(
  spark.table("shm.default.chunked_docs")
  .withColumn("chunk_id", F.monotonically_increasing_id())
  .write.format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")  # Enable schema merging
  .saveAsTable("shm.default.chunked_docs")
)

df = spark.table('shm.default.chunked_docs')
display(df.limit(5))

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# Get the vector search index
vsc = VectorSearchClient(disable_notice=True)

# COMMAND ----------

endpoint = 'one-env-shared-endpoint-4'
index_name = 'shm.default.vectorsearch_index'
force_delete = False

def find_index(vsc, endpoint_name, index_name):
    all_indexes = vsc.list_indexes(name=endpoint_name).get("vector_indexes", [])
    return index_name in map(lambda i: i.get("name"), all_indexes)
  
if find_index(
  vsc,
  endpoint_name=endpoint, 
  index_name=index_name):
    if force_delete:
        vsc.delete_index(
          endpoint_name=endpoint, index_name=index_name
          )
        create_index = True
    else:
        create_index = False
else:
    create_index = True

if create_index:
    vsc.create_delta_sync_index_and_wait(
        endpoint_name=endpoint,
        index_name=index_name,
        primary_key="chunk_id",
        source_table_name='shm.default.chunked_docs',
        pipeline_type='TRIGGERED',
        embedding_source_column="text",
        embedding_model_endpoint_name='databricks-bge-large-en'
    )

# COMMAND ----------

# Setup the index as a LangChain retriever
vs_index = vsc.get_index(
  endpoint_name=endpoint, 
  index_name=index_name
)

# COMMAND ----------

# Import the necessary class
from langchain.embeddings import DatabricksEmbeddings
from langchain_community.vectorstores import DatabricksVectorSearch

# Create the retriever
# 'k' is the number of results to return
embedding_model = DatabricksEmbeddings(
    endpoint="databricks-bge-large-en"
)

vs_retriever = DatabricksVectorSearch(
    vs_index, 
    text_column="text"
).as_retriever(search_kwargs={"k": 1})

# COMMAND ----------

vs_retriever.invoke("NON-DISCRIMINATORY WORKING ENVIRONMENT")

# COMMAND ----------


