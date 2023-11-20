import streamlit as st

from pyspark.sql import SparkSession
spark_session = SparkSession.builder.master("local").appName("article").getOrCreate()

@st.cache_data
def load_data():
    #health_twitter = spark_session.sparkContext.textFile("./health_twitter/*.txt")
    bbchealth = spark_session.sparkContext.textFile("./health_twitter/bbchealth.txt")
    cbchealth = spark_session.sparkContext.textFile("./health_twitter/cbchealth.txt")
    cnnhealth = spark_session.sparkContext.textFile("./health_twitter/cnnhealth.txt")
    everydayhealth = spark_session.sparkContext.textFile("./health_twitter/everydayhealth.txt")
    gdnhealthcare = spark_session.sparkContext.textFile("./health_twitter/gdnhealthcare.txt")
    goodhealth = spark_session.sparkContext.textFile("./health_twitter/goodhealth.txt")
    latimeshealth = spark_session.sparkContext.textFile("./health_twitter/latimeshealth.txt")
    nprhealth = spark_session.sparkContext.textFile("./health_twitter/nprhealth.txt")
    nytimeshealth = spark_session.sparkContext.textFile("./health_twitter/nytimeshealth.txt")
    reuters_health = spark_session.sparkContext.textFile("./health_twitter/reuters_health.txt")
    usnewshealth = spark_session.sparkContext.textFile("./health_twitter/usnewshealth.txt")
    health_twitter = bbchealth.union(cbchealth).union(cnnhealth).union(everydayhealth).union(gdnhealthcare).union(goodhealth).union(latimeshealth).union(nprhealth).union(nytimeshealth).union(reuters_health).union(usnewshealth)

    return health_twitter