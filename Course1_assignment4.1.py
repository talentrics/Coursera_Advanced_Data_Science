
# coding: utf-8

# # Assignment 4
# 
# Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook. Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# Sampling is one of the most important things when it comes to visualization because often the data set get so huge that you simply
# 
# - can't copy all data to a local Spark driver (Watson Studio is using a "local" Spark driver)
# - can't throw all data at the plotting library
# 
# Please implement a function which returns a 10% sample of a given data frame:

# In[399]:


def getSample(df,spark):
    sample=df.sample(False,0.1)
    return sample


# Now we want to create a histogram and boxplot. Please ignore the sampling for now and retur a python list containing all temperature values from the data set

# In[1]:


def getListForHistogramAndBoxPlot(df,spark):
    temprdd1 = df.select('temperature').rdd #in row(temp=x) format
    temprdd2 = temprdd1.map(lambda x : x["temperature"]) #only numbers
    temp = temprdd2.filter(lambda x: x is not None).filter(lambda x: x != "")
    return temp.collect()


# Finally we want to create a run chart. Please return two lists (encapusalted in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refere to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm

# In[111]:


#should return a tuple containing the two lists for timestamp and temperature
#please make sure you take only 10% of the data by sampling
#please also ensure that you sample in a way that the timestamp samples and temperature samples correspond (=> call sample on an object still containing both dimensions)
def getListsForRunChart(df,spark):
    temp = df.select('ts','temperature').sort(("ts")) #in row(temp=x) format
    #temp_2 = temp.na.drop(subset=["temperature"]).sort(("ts"))
    #temp_3 = temp_2.na.drop(subset=["temperature"])
    #temp_4 = temp_3.sample(False,0.1)
    temp_rdd1 = temp.na.drop(subset=["temperature"]).rdd
    temp_rdd2 = temp_rdd1.sample(False,0.1).map(lambda row:(row.ts,row.temperature)) #only numbers
    #temp_rdd3 = temp_rdd2.reducePairKey.collect()
    ts=temp_rdd2.keys().collect()
    temperature=temp_rdd2.values().collect()
    x=ts,temperature
    return x


# In[112]:


getListsForRunChart(df,spark)


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the object store and read a PARQUET file and create a dataframe out of it. We've created that data for you already. Using SparkSQL you can handle it like a database.

# In[4]:


import ibmos2spark

# @hidden_cell
credentials = {
    'endpoint': 'https://s3-api.us-geo.objectstorage.service.networklayer.com',
    'api_key': 'PUJMZf9PLqN4y-6NUtVlEuq6zFoWhfuecFVMYLBrkxrT',
    'service_id': 'iam-ServiceId-9cd8e66e-3bb4-495a-807a-588692cca4d0',
    'iam_service_endpoint': 'https://iam.bluemix.net/oidc/token'}

configuration_name = 'os_b0f1407510994fd1b793b85137baafb8_configs'
cos = ibmos2spark.CloudObjectStorage(sc, credentials, configuration_name, 'bluemix_cos')

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Since JSON data can be semi-structured and contain additional metadata, it is possible that you might face issues with the DataFrame layout.
# Please read the documentation of 'SparkSession.read()' to learn more about the possibilities to adjust the data loading.
# PySpark documentation: http://spark.apache.org/docs/2.0.2/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.json

df = spark.read.parquet(cos.url('washing.parquet', 'courseradsnew-donotdelete-pr-1hffrnl2pprwut'))
df.show()


# In[5]:


get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt


# In[6]:


plt.hist(getListForHistogramAndBoxPlot(df,spark))
plt.show()


# In[7]:


plt.boxplot(getListForHistogramAndBoxPlot(df,spark))
plt.show()


# In[113]:


lists = getListsForRunChart(df,spark)


# In[114]:


plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()


# Congratulations, you are done! Please download the notebook as python file, name it assignment4.1.py and sumbit it to the grader.
