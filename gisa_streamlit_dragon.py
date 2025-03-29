import streamlit as st
import pandas as pd
import datetime
import time
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os
import urllib.request

# GitHub에서 폰트 파일 다운로드
font_url = 'https://raw.githubusercontent.com/yongdragonee/semicon/main/NanumGothicCoding.ttf'
font_path = './NanumGothicCoding.ttf'
if not os.path.exists(font_path):
    urllib.request.urlretrieve(font_url, font_path)
fontprop = fm.FontProperties(fname=font_path)

# ===============================================
# 1. CSV 불러오기 함수 (기존 코드와 동일)
# ===============================================
def load_data(csv_url):
    df = pd.read_csv(csv_url, encoding='utf-8-sig')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.sort_values(by='date', ascending=False)
    def split_keywords(kw_string):
        if pd.isna(kw_string):
            return []
        return [k.strip() for k in kw_string.split(',') if k.strip()]
    df['키워드_목록'] = df['키워드'].apply(split_keywords)
    df = df.explode('키워드_목록', ignore_index=True)
    df['키워드_목록'] = df['키워드_목록'].replace('관련 없음', '기타')
    return df

# ===============================================
# 2. 데이터 로드
# ===============================================
GITHUB_CSV_URL = st.secrets["CSV_URL"] + f"?nocache={int(time.time())}"
df = load_data(GITHUB_CSV_URL)

# 날짜 범위 초기값 (최근 7일, 1달 등)
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
unique_dates = sorted(list(set(df['date'].dt.date.dropna())), reverse=True)
if date_filter_option == "최근 7일":
    selected_dates = [d for d in unique_dates if d >= one_week_ago.date()]
elif date_filter_option == "최근 1달":
    selected_dates = [d for d in unique_dates if d >= one_month_ago.date()]
elif date_filter_option == "전체":
    selected_dates = unique_dates
else:
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
# 5. 주가 정보 조회 - yfinance 사용 (최근 1년, Dual Y-Axis 그래프)
# ===============================================
st.header("📈 주가 정보 조회 (최근 1년) - Dual Y-Axis 그래프")
today = datetime.date.today()
start_date_for_yf = today - datetime.timedelta(days=365)
end_date_for_yf = today + datetime.timedelta(days=1)
col1, col2 = st.columns(2)

# 좌측: 코스피지수 / 코스닥지수 (Dual Y-Axis)
with col1:
    st.subheader("코스피/코스닥")
    left_tickers = {"코스피지수": "^KS11", "코스닥지수": "^KQ11"}
    left_list = list(left_tickers.values())
    try:
        left_data = yf.download(left_list, start=start_date_for_yf, end=end_date_for_yf)
        if left_data.empty:
            st.warning("해당 기간에 대한 코스피/코스닥 데이터가 없습니다.")
        else:
            if isinstance(left_data.columns, pd.MultiIndex):
                close_left = left_data['Close']
            else:
                close_left = left_data[['Close']]
            # 티커명을 한글로 변경
            ticker_map_left = {v: k for k, v in left_tickers.items()}
            close_left.rename(columns=ticker_map_left, inplace=True)
            
            # dual y-axis 플롯 생성
            fig, ax1 = plt.subplots(figsize=(8,4))
            ax1.plot(close_left.index, close_left["코스피지수"], color='blue', label="코스피지수")
            ax1.set_ylabel("코스피지수", color='blue', fontproperties=fontprop)
            ax1.tick_params(axis='y', labelcolor='blue')
            ax2 = ax1.twinx()
            ax2.plot(close_left.index, close_left["코스닥지수"], color='red', label="코스닥지수")
            ax2.set_ylabel("코스닥지수", color='red', fontproperties=fontprop)
            ax2.tick_params(axis='y', labelcolor='red')
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', prop=fontprop)
            ax1.set_title("코스피/코스닥 지수", fontproperties=fontprop)
            st.pyplot(fig)
            
            # ----- 코스피/코스닥 주가 변동률 계산 및 출력 -----
            st.markdown("#### 코스피/코스닥 주가 변동률")
            left_pct_changes = {}
            for ticker in ["코스피지수", "코스닥지수"]:
                # 데이터의 인덱스를 날짜순(오름차순)으로 정렬
                series = close_left[ticker].dropna().sort_index()
                changes = {}
                # 최근 1일: 최신 거래일과 바로 전 거래일 비교 (라벨은 최신 거래일 날짜로 표시)
                if len(series) >= 2:
                    latest_date = series.index[-1]
                    prev_date = series.index[-2]
                    changes[f"최근1일 ({latest_date.strftime('%Y-%m-%d')})"] = (series.iloc[-1] / series.iloc[-2] - 1) * 100
                else:
                    changes["최근1일"] = None
                # 최근 7일: 최신 거래일 기준 7일 전 이하의 가장 가까운 거래일
                if len(series) > 0:
                    latest_date = series.index[-1]
                    candidate7 = series[series.index <= latest_date - pd.Timedelta(days=7)]
                    if len(candidate7) > 0:
                        changes["최근7일"] = (series.iloc[-1] / candidate7.iloc[-1] - 1) * 100
                    else:
                        changes["최근7일"] = None
                    # 최근 1달: 30일 전 이하의 가장 가까운 거래일
                    candidate30 = series[series.index <= latest_date - pd.Timedelta(days=30)]
                    if len(candidate30) > 0:
                        changes["최근1달"] = (series.iloc[-1] / candidate30.iloc[-1] - 1) * 100
                    else:
                        changes["최근1달"] = None
                    # 최근 1년: 365일 전 이하의 가장 가까운 거래일
                    candidate365 = series[series.index <= latest_date - pd.Timedelta(days=365)]
                    if len(candidate365) > 0:
                        changes["최근1년"] = (series.iloc[-1] / candidate365.iloc[-1] - 1) * 100
                    else:
                        changes["최근1년"] = None
                else:
                    changes["최근7일"] = changes["최근1달"] = changes["최근1년"] = None
                left_pct_changes[ticker] = changes

            for ticker, changes in left_pct_changes.items():
                st.write(f"**{ticker} 주가 변동률**")
                for period, pct in changes.items():
                    if pct is not None:
                        st.write(f"- {period}: {pct:.2f}%")
                    else:
                        st.write(f"- {period}: 데이터 부족")
    except Exception as e:
        st.error(f"코스피/코스닥 데이터를 가져오는 중 오류 발생: {e}")

# 우측: 삼성전자 / SK하이닉스 (Dual Y-Axis)
with col2:
    st.subheader("삼성전자 / SK하이닉스")
    right_tickers = {"삼성전자": "005930.KS", "SK하이닉스": "000660.KS"}
    right_list = list(right_tickers.values())
    try:
        right_data = yf.download(right_list, start=start_date_for_yf, end=end_date_for_yf)
        if right_data.empty:
            st.warning("해당 기간에 대한 삼성전자/SK하이닉스 데이터가 없습니다.")
        else:
            if isinstance(right_data.columns, pd.MultiIndex):
                close_right = right_data['Close']
            else:
                close_right = right_data[['Close']]
            ticker_map_right = {v: k for k, v in right_tickers.items()}
            close_right.rename(columns=ticker_map_right, inplace=True)
            
            fig, ax1 = plt.subplots(figsize=(8,4))
            ax1.plot(close_right.index, close_right["삼성전자"], color='blue', label="삼성전자")
            ax1.set_ylabel("삼성전자", color='blue', fontproperties=fontprop)
            ax1.tick_params(axis='y', labelcolor='blue')
            ax2 = ax1.twinx()
            ax2.plot(close_right.index, close_right["SK하이닉스"], color='red', label="SK하이닉스")
            ax2.set_ylabel("SK하이닉스", color='red', fontproperties=fontprop)
            ax2.tick_params(axis='y', labelcolor='red')
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', prop=fontprop)
            ax1.set_title("삼성전자 / SK하이닉스", fontproperties=fontprop)
            st.pyplot(fig)
            
            st.markdown("#### 삼성전자 / SK하이닉스 주가 변동률")
            right_pct_changes = {}
            for ticker in ["삼성전자", "SK하이닉스"]:
                series = close_right[ticker].dropna().sort_index()
                changes = {}
                if len(series) >= 2:
                    latest_date = series.index[-1]
                    prev_date = series.index[-2]
                    changes[f"최근1일 ({latest_date.strftime('%Y-%m-%d')})"] = (series.iloc[-1] / series.iloc[-2] - 1) * 100
                else:
                    changes["최근1일"] = None
                if len(series) > 0:
                    latest_date = series.index[-1]
                    candidate7 = series[series.index <= latest_date - pd.Timedelta(days=7)]
                    if len(candidate7) > 0:
                        changes["최근7일"] = (series.iloc[-1] / candidate7.iloc[-1] - 1) * 100
                    else:
                        changes["최근7일"] = None
                    candidate30 = series[series.index <= latest_date - pd.Timedelta(days=30)]
                    if len(candidate30) > 0:
                        changes["최근1달"] = (series.iloc[-1] / candidate30.iloc[-1] - 1) * 100
                    else:
                        changes["최근1달"] = None
                    candidate365 = series[series.index <= latest_date - pd.Timedelta(days=365)]
                    if len(candidate365) > 0:
                        changes["최근1년"] = (series.iloc[-1] / candidate365.iloc[-1] - 1) * 100
                    else:
                        changes["최근1년"] = None
                else:
                    changes["최근7일"] = changes["최근1달"] = changes["최근1년"] = None
                right_pct_changes[ticker] = changes

            for ticker, changes in right_pct_changes.items():
                st.write(f"**{ticker} 주가 변동률**")
                for period, pct in changes.items():
                    if pct is not None:
                        st.write(f"- {period}: {pct:.2f}%")
                    else:
                        st.write(f"- {period}: 데이터 부족")

    except Exception as e:
        st.error(f"삼성전자/SK하이닉스 데이터를 가져오는 중 오류 발생: {e}")

# ===============================================
# 6. 종합 주가 변동률 표 출력 (추가)
# ===============================================
st.markdown("### 종합 주가 변동률 표")

# 좌측과 우측의 변동률 딕셔너리를 하나로 결합
all_pct_changes = {}

def simplify_keys(changes):
    # "최근1일 (날짜)" 같은 키를 "최근1일"로 간단하게 변환
    simplified = {}
    for k, v in changes.items():
        if "최근1일" in k:
            simplified["최근1일"] = v
        else:
            simplified[k] = v
    return simplified

for ticker, changes in left_pct_changes.items():
    all_pct_changes[ticker] = simplify_keys(changes)
for ticker, changes in right_pct_changes.items():
    all_pct_changes[ticker] = simplify_keys(changes)

# DataFrame으로 변환 (행: 종목, 열: 기간)
pct_df = pd.DataFrame(all_pct_changes).T
# 원하는 순서로 열 재정렬
pct_df = pct_df[['최근1일', '최근7일', '최근1달', '최근1년']]

# 소수점 2자리 포맷 적용하여 표 출력
st.dataframe(pct_df.style.format("{:.2f}%"))

# ===============================================
# 7. 뉴스 출력
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
