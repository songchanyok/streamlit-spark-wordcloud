import sys, re
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

from wordcloud import WordCloud

import streamlit as st
from streamlit_option_menu import option_menu
from wordcloudpage import run_wordcloud

def main():
    with st.sidebar:
        selected = option_menu("대시보드 메뉴",['홈','워드클라우드'], icons=['house','file-bar-graph'], 
                               menu_icon="cast",default_index=0)
    if selected=="홈":
        st.title('대시보드 개요')
    elif selected =="워드클라우드":
        run_wordcloud()
    else:
        print("error")
if __name__ =="__main__":
    main()


  
        











