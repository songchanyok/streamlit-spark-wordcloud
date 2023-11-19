import sys, re
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

from wordcloud import WordCloud

import streamlit as st
from streamlit_option_menu import option_menu

def main():
    with st.sidebar:
        selected = option_menu("대시보드 메뉴",['홈','워드클라우드'], icons=['house','file-bar-graph'], 
                               menu_icon="cast",default_index=0)
    if selected=="홈":
        st.title('대시보드 개요')
    elif selected =="워드클라우드":
        pass
    else:
        print("error")
if __name__ =="__main__":
    main()
#spark_session = SparkSession.builder.master("local").appName("article").getOrCreate()

#health_twitter = spark_session.sparkContext.textFile("./health_twitter/*.txt")
# bbchealth = spark_session.sparkContext.textFile("./health_twitter/bbchealth.txt")
# cbchealth = spark_session.sparkContext.textFile("./health_twitter/cbchealth.txt")
# cnnhealth = spark_session.sparkContext.textFile("./health_twitter/cnnhealth.txt")
# everydayhealth = spark_session.sparkContext.textFile("./health_twitter/everydayhealth.txt")
# gdnhealthcare = spark_session.sparkContext.textFile("./health_twitter/gdnhealthcare.txt")
# goodhealth = spark_session.sparkContext.textFile("./health_twitter/goodhealth.txt")
# latimeshealth = spark_session.sparkContext.textFile("./health_twitter/latimeshealth.txt")
# nprhealth = spark_session.sparkContext.textFile("./health_twitter/nprhealth.txt")
# nytimeshealth = spark_session.sparkContext.textFile("./health_twitter/nytimeshealth.txt")
# reuters_health = spark_session.sparkContext.textFile("./health_twitter/reuters_health.txt")
# usnewshealth = spark_session.sparkContext.textFile("./health_twitter/usnewshealth.txt")
def wordcounts(x): 
    rdd=x.filter(lambda line: len(line) > 0) \
        .map(lambda x: x.split('|')).map(lambda x: (x[1].split(' ')[-1],x[2])) \
        .map(lambda x: (x[0],re.split('\W+',x[1]))) \
        .map(lambda x: (x[0],[n.lower() for n in x[1]])) \
        .map(lambda x: (x[0],[(x[1][:n]) for n in range(len(x[1])) if x[1][n]=='http'])) \
        .flatMapValues(lambda x: x) \
        .map(lambda x:(x[0],[(n,1) for n in x[1] if len(n)>3 \
            and n not in ['your','with','health','healthy','these','this','what','that','from','here','have','more','about','http','well','some','will','says','p2yzyb','video','should','could','just','than','year','time','week','today','doctors','doctor','people','patients','patient','life','like','after','when','2015','gdnhealthcare','ways','tips','help','good','better','most','best','over','make','know','find','finds','need','foods','want','they','their','disease','recipe','medical','fight','first','care','read','linked','case','things','back','outbreak','learn','think','take','into','miss']])) \
        .reduceByKey(lambda v1, v2: v1 + v2) 
        
    top_keyword = rdd.take(5)
    top_keyword_2015=[n for n in top_keyword if n[0] == '2015']
    top_keyword_2014=[n for n in top_keyword if n[0] == '2014']
    top_keyword_2013=[n for n in top_keyword if n[0] == '2013']
    top_keyword_2012=[n for n in top_keyword if n[0] == '2012']
    top_keyword_2011=[n for n in top_keyword if n[0] == '2011']

    rdd_2011= spark_session.sparkContext.parallelize(top_keyword_2011)
    rdd_2012= spark_session.sparkContext.parallelize(top_keyword_2012)
    rdd_2013= spark_session.sparkContext.parallelize(top_keyword_2013)
    rdd_2014= spark_session.sparkContext.parallelize(top_keyword_2014)
    rdd_2015= spark_session.sparkContext.parallelize(top_keyword_2015)

    rdd_2011_rank=rdd_2011.flatMap(lambda x: x[1]) \
                            .reduceByKey(lambda x,y:x+y) \
                            .sortBy(lambda x: x[1], ascending=False)
    rdd_2012_rank=rdd_2012.flatMap(lambda x: x[1]) \
                            .reduceByKey(lambda x,y:x+y) \
                            .sortBy(lambda x: x[1], ascending=False)    
    rdd_2013_rank=rdd_2013.flatMap(lambda x: x[1]) \
                            .reduceByKey(lambda x,y:x+y) \
                            .sortBy(lambda x: x[1], ascending=False)
    rdd_2014_rank=rdd_2014.flatMap(lambda x: x[1]) \
                            .reduceByKey(lambda x,y:x+y) \
                            .sortBy(lambda x: x[1], ascending=False)
    rdd_2015_rank=rdd_2015.flatMap(lambda x: x[1]) \
                            .reduceByKey(lambda x,y:x+y) \
                            .sortBy(lambda x: x[1], ascending=False) 
    
    return rdd_2011_rank, rdd_2012_rank, rdd_2013_rank, rdd_2014_rank, rdd_2015_rank    
        


# health_twit2011, health_twit2012, health_twit2013, health_twit2014, health_twit2015 =wordcounts(health_twitter)

# health_twit2011_top20words = dict(health_twit2011.take(20))
# health_twit2012_top20words = dict(health_twit2012.take(20))
# health_twit2013_top20words = dict(health_twit2013.take(20))
# health_twit2014_top20words = dict(health_twit2014.take(20))
# health_twit2015_top20words = dict(health_twit2015.take(20))

# print(health_twit2011_top20words)
# print(health_twit2012_top20words)
# print(health_twit2013_top20words)
# print(health_twit2014_top20words)
# print(health_twit2015_top20words)

# justwords_2011 = []
# justwords_2012 = []
# justwords_2013 = []
# justwords_2014 = []
# justwords_2015 = []

# for wordsandcounts in health_twit2011_top20words.keys():
#     justwords_2011.append(wordsandcounts)
# for wordsandcounts in health_twit2012_top20words.keys():
#     justwords_2012.append(wordsandcounts)
# for wordsandcounts in health_twit2013_top20words.keys():
#     justwords_2013.append(wordsandcounts)
# for wordsandcounts in health_twit2014_top20words.keys():
#     justwords_2014.append(wordsandcounts)
# for wordsandcounts in health_twit2015_top20words.keys():
#     justwords_2015.append(wordsandcounts)
# print(" 2011 The top 20 words are : " + str(justwords_2011))
# print(" 2012 The top 20 words are : " + str(justwords_2012))
# print(" 2013 The top 20 words are : " + str(justwords_2013))
# print(" 2014 The top 20 words are : " + str(justwords_2014))
# print(" 2015 The top 20 words are : " + str(justwords_2015))

# #plt.rc('font', family='NanumGothic')
# path = './font/NanumGothic.ttf'
# wc = WordCloud(font_path = path,
#                background_color='white',
#                width=1000,
#                height=1000,
#                max_font_size=300)

# dict_list = [health_twit2011_top20words,health_twit2012_top20words,health_twit2013_top20words,health_twit2014_top20words,health_twit2015_top20words]
# title_list = [2011,2012,2013,2014,2015]

# fig = plt.figure(figsize=(25,20))
# for i in range(len(dict_list)):
#     wc.generate_from_frequencies(dict_list[i]) #워드클라우드 생성
#     ax = fig.add_subplot(2,3,i+1)
#     ax.imshow(wc, interpolation='bilinear')
#     ax.set_xlabel(f'{title_list[i]}') #그래프 제목 출력
#     ax.set_xticks([]), ax.set_yticks([]) #x축, y축을 없앰
#     plt.imshow(wc, interpolation='bilinear')

# fig.suptitle('twitter health news top keyword')
# fig.tight_layout()
# plt.show()