import pandas as pd
import streamlit as st

def run_home():
    st.markdown("## 데이터 \n"
                "트위터 뉴스기사 원본 파일입니다. 👇👇👇")
    
    st.subheader('txt 파일 업로드')
    with open("./health_twitter.zip","rb") as fp:
        st.download_button(
            label="Download data ZIP",
            data = fp,
            file_name='health_twitter.zip',
            mime="application/zip"
        )
    