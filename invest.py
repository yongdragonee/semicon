import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

GITHUB_CSV_URL = st.secrets["CSV_URL"] + f"?nocache={int(time.time())}"
news_df = load_news_data(GITHUB_CSV_URL)

GITHUB_REPORT_URL = st.secrets["REPORT_URL"] + f"?nocache={int(time.time())}"
report_df = load_report_data(GITHUB_REPORT_URL)

# NLTK의 vader_lexicon 다운로드 (최초 실행 시에만)
nltk.download('vader_lexicon')

# VADER Sentiment Analyzer 초기화
sia = SentimentIntensityAnalyzer()

# 데이터 로딩 함수 (Streamlit 캐싱 이용)
@st.cache_data
def load_data():
    df = pd.read_csv('reports.csv')
    # 'date' 컬럼이 있다면 datetime 형식으로 변환
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df

# 감정 분석 수행 함수: 요약(summary) 기준으로 분석 (요약이 없을 경우 content 사용 가능)
def compute_sentiment(text):
    if pd.isnull(text):
        return None
    return sia.polarity_scores(text)

# 데이터 불러오기
data = load_data()

# 감정 분석 결과 컬럼 추가: 'summary' 컬럼 기준 (필요에 따라 'content'로 대체 가능)
if 'summary' in data.columns:
    sentiment_scores = data['summary'].apply(compute_sentiment)
    # sentiment_scores는 Series로 각 원소가 dict 형태입니다. 이를 개별 컬럼으로 풀어넣습니다.
    sentiment_df = sentiment_scores.apply(pd.Series)
    # 결과 DataFrame을 원본 데이터에 합칩니다.
    data = pd.concat([data, sentiment_df], axis=1)

# 사이드바 - 필터 옵션
st.sidebar.header("필터 옵션")

# 날짜 필터 (date 컬럼이 있을 경우)
if 'date' in data.columns:
    min_date = data['date'].min()
    max_date = data['date'].max()
    date_range = st.sidebar.date_input("날짜 범위 선택", [min_date, max_date])
    if len(date_range) == 2:
        start_date, end_date = date_range
        data = data[(data['date'] >= pd.Timestamp(start_date)) & (data['date'] <= pd.Timestamp(end_date))]

# 업종/섹터 필터 (컬럼명이 'sector' 인 경우)
if 'sector' in data.columns:
    sectors = data['sector'].unique().tolist()
    selected_sectors = st.sidebar.multiselect("업종 선택", sectors, default=sectors)
    data = data[data['sector'].isin(selected_sectors)]

# 키워드 검색 (제목과 요약 기준)
keyword = st.sidebar.text_input("키워드 검색 (제목, 요약)")
if keyword:
    data = data[
        data['title'].str.contains(keyword, case=False, na=False) |
        data['summary'].str.contains(keyword, case=False, na=False)
    ]

st.title("증권 레포트 모음 및 분석")
st.write("### 데이터 미리보기")
st.dataframe(data.head())

# 전체 감정 분석 결과 시각화 (Compound 점수 분포)
if 'compound' in data.columns:
    st.write("### 전체 레포트 감정 분석 분포")
    fig, ax = plt.subplots(figsize=(8,4))
    ax.hist(data['compound'].dropna(), bins=20)
    ax.set_title("Compound 점수 분포")
    ax.set_xlabel("Compound 점수")
    ax.set_ylabel("레포트 수")
    st.pyplot(fig)

# 왼쪽에서 보고 싶은 레포트를 선택해서 상세보기
st.write("### 상세 레포트 보기")
if not data.empty:
    # DataFrame의 인덱스를 선택할 수 있도록 설정
    selected_index = st.selectbox("레포트 선택", data.index)
    report = data.loc[selected_index]
    
    st.subheader(report.get('title', '제목 없음'))
    if 'date' in report:
        st.text("날짜: " + str(report['date'].date()))
    if 'analyst' in report:
        st.text("애널리스트: " + str(report['analyst']))
    if 'sector' in report:
        st.text("업종: " + str(report['sector']))
        
    # 요약과 내용 출력
    if 'summary' in report:
        st.write("**요약**")
        st.write(report['summary'])
    if 'content' in report:
        st.write("**레포트 내용**")
        st.write(report['content'])
    
    # 감정 분석 결과 출력 (요약 텍스트 기준)
    if 'compound' in report and pd.notnull(report['compound']):
        st.write("**감정 분석 결과**")
        st.write(f"긍정 (pos): {report['pos']:.2f}")
        st.write(f"중립 (neu): {report['neu']:.2f}")
        st.write(f"부정 (neg): {report['neg']:.2f}")
        st.write(f"종합 (compound): {report['compound']:.2f}")
    else:
        st.write("감정 분석 결과가 없습니다.")
else:
    st.write("필터링 결과가 없습니다.")
