import streamlit as st
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

# NLTK의 VADER 감정 분석 도구 다운로드
nltk.download('vader_lexicon')

# SentimentIntensityAnalyzer 초기화
sia = SentimentIntensityAnalyzer()

# Streamlit 애플리케이션 제목 설정
st.title('감정 분석 애플리케이션')

# 사용자로부터 텍스트 입력 받기
user_input = st.text_area('텍스트를 입력하세요:', '')

# 감정 분석 수행 및 결과 표시
if st.button('분석하기'):
    if user_input:
        sentiment_score = sia.polarity_scores(user_input)
        compound_score = sentiment_score['compound']
        if compound_score >= 0.05:
            st.success('긍정적인 텍스트입니다.')
        elif compound_score <= -0.05:
            st.error('부정적인 텍스트입니다.')
        else:
            st.warning('중립적인 텍스트입니다.')
    else:
        st.warning('텍스트를 입력해주세요.')
