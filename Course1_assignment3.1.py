
# coding: utf-8

# # Assignment 3
# 
# Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures on the test data you have created.
# 
# YOU ARE NOT ALLOWED TO USE ANY OTHER 3RD PARTY LIBRARIES LIKE PANDAS. PLEASE ONLY MODIFY CONTENT INSIDE THE FUNCTION SKELETONS
# Please read why: https://www.coursera.org/learn/exploring-visualizing-iot-data/discussions/weeks/3/threads/skjCbNgeEeapeQ5W6suLkA
# . Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. 
# 
# Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. You can use this skeleton for all subsequent functions. Everything can be implemented using SQL only if you like.

# In[138]:


def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "org.apache.bahir.cloudant")

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# In[154]:


def minTemperature(df,spark):
    min=df.agg({"temperature": "min"}).collect()[0]
    min_temp = min["min(temperature)"]
    return min_temp


# Please now do the same for the mean of the temperature

# In[156]:


def meanTemperature(df,spark):
    mean=df.agg({"temperature": "avg"}).collect()[0]
    mean_temp = mean["avg(temperature)"]
    return mean_temp


# Please now do the same for the maximum of the temperature

# In[157]:


def maxTemperature(df,spark):
    max=df.agg({"temperature": "max"}).collect()[0]
    max_temp = max["max(temperature)"]
    return max_temp


# Please now do the same for the standard deviation of the temperature

# In[158]:


def sdTemperature(df,spark):
    temprdd1 = df.select('temperature').rdd #in row(temp=x) format
    temprdd2 = temprdd1.map(lambda x : x["temperature"]) #only numbers
    temp = temprdd2.filter(lambda x: x is not None).filter(lambda x: x != "") #remove None params
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    return sd


# Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four position in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.

# In[159]:


def skewTemperature(df,spark): 
    temprdd1 = df.select('temperature').rdd #in row(temp=x) format
    temprdd2 = temprdd1.map(lambda x : x["temperature"]) #only numbers
    temp = temprdd2.filter(lambda x: x is not None).filter(lambda x: x != "")  #remove None params
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    skew=n*(temp.map(lambda x:pow(x-mean,3)/pow(sd,3)).sum())/(float(n-1)*float(n-2))
    return skew


# Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.

# In[160]:


def kurtosisTemperature(df,spark): 
    temprdd1 = df.select('temperature').rdd
    temprdd2 = temprdd1.map(lambda x : x["temperature"])
    temp = temprdd2.filter(lambda x: x is not None).filter(lambda x: x != "")
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    kurtosis=temp.map(lambda x:pow(x-mean,4)).sum()/(pow(sd,4)*(n))
    return kurtosis


# Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.

# In[161]:


def correlationTemperatureHardness(df,spark):
    temprdd1 = df.select('temperature').rdd
    temprdd2 = temprdd1.map(lambda x : x["temperature"])
    temp = temprdd2.filter(lambda x: x is not None).filter(lambda x: x != "")
    hardrdd1 = df.select('hardness').rdd
    hardrdd2 = hardrdd1.map(lambda x : x["hardness"])
    hard = hardrdd2.filter(lambda x: x is not None).filter(lambda x: x != "")
    n = float(temp.count())
    sumt=temp.sum()
    sumh=hard.sum()
    meant =sumt/n
    meanh = sumh/n
    cov_temp1 = temp.map(lambda x : x-meant)
    cov_temp2 = cov_temp1.filter(lambda x: x is not None).filter(lambda x: x != "")
    cov_temp_sum = cov_temp2.sum()
    cov_hard1 = hard.map(lambda x : x-meanh)
    cov_hard2 = cov_hard1.filter(lambda x: x is not None).filter(lambda x: x != "")
    cov_hard_sum = cov_hard2.sum()
    cov12 = (cov_temp_sum*cov_hard_sum)/n
    from math import sqrt
    sd1 = sqrt(temp.map(lambda x : pow(x-meant,2)).sum()/n)
    sd2 = sqrt(hard.map(lambda x : pow(x-meanh,2)).sum()/n)
    sdx = float(sd1*sd2)
    corr12 = cov12 / sdx
    return corr12


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the object store and read a PARQUET file and create a dataframe out of it. We've created that data for you already. Using SparkSQL you can handle it like a database.

# In[162]:


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


# In[163]:


minTemperature(df,spark)


# In[164]:


meanTemperature(df,spark)


# In[165]:


maxTemperature(df,spark)


# In[166]:


sdTemperature(df,spark)


# In[167]:


skewTemperature(df,spark)


# In[168]:


kurtosisTemperature(df,spark)


# In[169]:


correlationTemperatureHardness(df,spark)


# Congratulations, you are done, please download this notebook as python file using the export function and submit is to the gader using the filename "assignment3.1.py"
