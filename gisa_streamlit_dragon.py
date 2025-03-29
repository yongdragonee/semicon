import streamlit as st
import pandas as pd
import datetime
import time
import yfinance as yf

# ===============================================
# 1. CSV 불러오기 함수 (기존 코드와 동일)
# ===============================================
def load_data(csv_url):
    df = pd.read_csv(csv_url, encoding='utf-8-sig')
    
    # 날짜 컬럼 변환 및 정렬
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.sort_values(by='date', ascending=False)
    
    # 키워드 분할
    def split_keywords(kw_string):
        if pd.isna(kw_string):
            return []
        return [k.strip() for k in kw_string.split(',') if k.strip()]
    
    df['키워드_목록'] = df['키워드'].apply(split_keywords)
    
    # explode
    df = df.explode('키워드_목록', ignore_index=True)
    
    # '관련 없음' → '기타'
    df['키워드_목록'] = df['키워드_목록'].replace('관련 없음', '기타')
    
    return df

# ===============================================
# 2. 데이터 로드
# ===============================================
GITHUB_CSV_URL = st.secrets["CSV_URL"] + f"?nocache={int(time.time())}"
df = load_data(GITHUB_CSV_URL)

# 날짜 범위 초기값(최근 7일, 1달 등)
if not df.empty:
    max_date = df['date'].max()
    one_week_ago = max_date - datetime.timedelta(days=7)
    one_month_ago = max_date - datetime.timedelta(days=30)
else:
    one_week_ago = one_month_ago = None

# ===============================================
# 3. 화면 구성
# ===============================================
st.title("📢 반도체 뉴스레터(Rev.25.3.13)")
st.write("문의/아이디어 : yh9003.lee@samsung.com")

# ---- 사이드바 날짜 필터 옵션 ----
date_filter_option = st.sidebar.radio(
    "📅 날짜 필터 옵션",
    ["최근 7일", "최근 1달", "전체", "직접 선택"],
    index=0
)

# 날짜 리스트 준비
unique_dates = sorted(list(set(df['date'].dt.date.dropna())), reverse=True)

if date_filter_option == "최근 7일":
    selected_dates = [d for d in unique_dates if d >= one_week_ago.date()]
elif date_filter_option == "최근 1달":
    selected_dates = [d for d in unique_dates if d >= one_month_ago.date()]
elif date_filter_option == "전체":
    selected_dates = unique_dates
else:  # "직접 선택"
    selected_dates = st.sidebar.multiselect(
        "📅 날짜를 선택하세요 (복수 선택 가능)",
        unique_dates,
        help="필터 옵션에서 '직접 선택'을 선택한 경우에만 활성화됩니다."
    )

# ---- 사이드바 키워드 필터 ----
unique_keywords = sorted(list(df['키워드_목록'].dropna().unique()))
selected_keywords = st.sidebar.multiselect(
    "🔍 키워드를 선택하세요 (복수 선택 가능)",
    unique_keywords,
    help="아무 것도 선택하지 않으면 모든 키워드가 표시됩니다."
)

# ---- 사이드바 검색어 필터 (제목/요약) ----
search_query = st.sidebar.text_input(
    "🔎 검색어 입력 (제목/요약 포함)",
    help="특정 단어가 포함된 기사만 검색합니다."
)

# ===============================================
# 4. 뉴스 데이터 필터링
# ===============================================
filtered_df = df.copy()

if selected_dates:
    filtered_df = filtered_df[filtered_df['date'].dt.date.isin(selected_dates)]

if selected_keywords:
    filtered_df = filtered_df[filtered_df['키워드_목록'].isin(selected_keywords)]

if search_query:
    search_query_lower = search_query.lower()
    filtered_df = filtered_df[
        filtered_df['title'].str.lower().str.contains(search_query_lower, na=False) |
        filtered_df['summary'].fillna('').str.lower().str.contains(search_query_lower, na=False)
    ]

st.write(f"**총 기사 수:** {len(filtered_df)}개")

# ===============================================
# 5. (추가) 주가 정보 조회 - yfinance 사용 (최근 1년)
# ===============================================
st.sidebar.write("---")
st.sidebar.write("**📈 주가 정보 조회**")

# 예: 삼성전자 코스피 티커 "005930.KS", TSMC "TSM", etc.
stock_ticker = st.sidebar.text_input("티커 입력 (예: 005930.KS)", value="005930.KS")

# 주가 데이터: 최근 1년 치로 고정
today = datetime.date.today()
start_date_for_yf = today - datetime.timedelta(days=365)
end_date_for_yf = today + datetime.timedelta(days=1)

if stock_ticker:
    try:
        stock_data = yf.download(
            stock_ticker,
            start=start_date_for_yf,
            end=end_date_for_yf
        )

        if not stock_data.empty:
            st.subheader(f"최근 1년 주가 추이: {stock_ticker}")

            # 단일 티커의 경우 'Close' 열만 선택하여 사용
            close_series = stock_data['Close']

            # 이미 index가 날짜이므로 별도 처리 없이 차트를 그릴 수 있음
            st.line_chart(close_series)

            with st.expander("주가 데이터 펼쳐보기"):
                st.dataframe(stock_data)
        else:
            st.warning("해당 기간에 대한 주가 데이터가 없습니다.")

    except Exception as e:
        st.error(f"주가 데이터를 가져오는 중 오류가 발생했습니다: {e}")

# ===============================================
# 6. 뉴스 출력
# ===============================================
grouped_by_date = filtered_df.groupby(filtered_df['date'].dt.date, sort=False)

for current_date, date_group in grouped_by_date:
    st.markdown(f"## {current_date.strftime('%Y-%m-%d')}")

    grouped_by_keyword = date_group.groupby('키워드_목록', sort=False)
    
    for keyword_value, keyword_group in grouped_by_keyword:
        if pd.notna(keyword_value) and str(keyword_value).strip():
            st.markdown(f"### ▶️ {keyword_value}")
        else:
            st.markdown("### ▶️ (키워드 없음)")
        
        for idx, row in keyword_group.iterrows():
            with st.expander(f"📰 {row['title']}"):
                st.write(f"**요약:** {row.get('summary', '요약 정보가 없습니다.')}")

                link = row.get('link', None)
                if pd.notna(link):
                    st.markdown(f"[🔗 기사 링크]({link})")
                else:
                    st.write("링크가 없습니다.")
