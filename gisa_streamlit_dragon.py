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
# 5. 주가 정보 조회 - yfinance 사용 (최근 1년)
# ===============================================
st.header("📈 주가 정보 조회 (최근 1년)")
# 4개 주가: 코스피지수, 코스닥지수, 삼성전자, SK하이닉스
stock_tickers = {
    "코스피지수": "^KS11",
    "코스닥지수": "^KQ11",
    "삼성전자": "005930.KS",
    "SK하이닉스": "000660.KS"
}

today = datetime.date.today()
start_date_for_yf = today - datetime.timedelta(days=365)
end_date_for_yf = today + datetime.timedelta(days=1)

for name, ticker in stock_tickers.items():
    st.subheader(f"{name} ({ticker})")
    try:
        stock_data = yf.download(
            ticker,
            start=start_date_for_yf,
            end=end_date_for_yf
        )
        if stock_data.empty:
            st.warning(f"{name}의 해당 기간 주가 데이터가 없습니다.")
            continue

        # 컬럼이 튜플 형태인지 확인 (예: ("Close", "005930.KS"))
        first_col = stock_data.columns[0]
        if isinstance(first_col, tuple):
            # 'Close'에 해당하는 튜플 컬럼 찾기
            close_cols = [col for col in stock_data.columns if col[0] == 'Close']
            if not close_cols:
                st.warning(f"{name}의 데이터에 'Close' 열이 없습니다.")
                continue
            # 티커별로 저장된 경우 해당 티커에 맞는 컬럼 선택
            close_cols_filtered = [col for col in close_cols if col[1] == ticker]
            target_col = close_cols_filtered[0] if close_cols_filtered else close_cols[0]
            close_data = stock_data[target_col].to_frame(name='Close')
        else:
            if 'Close' in stock_data.columns:
                # 단일 티커의 경우, 'Close' 열만 선택 후 컬럼 이름 재정의
                close_data = stock_data[['Close']]
                close_data.columns = ['Close']
            else:
                st.warning(f"{name}의 데이터에 'Close' 열이 없습니다.")
                continue

        st.line_chart(close_data)
        with st.expander("주가 데이터 펼쳐보기"):
            st.dataframe(close_data)
    except Exception as e:
        st.error(f"{name} 주가 데이터를 가져오는 중 오류가 발생했습니다: {e}")

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
