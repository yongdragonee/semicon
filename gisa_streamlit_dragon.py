import streamlit as st
st.set_page_config(page_title="반도체 뉴스레터", layout="wide")   # ← 반드시 첫 st.* 호출
import pandas as pd
import datetime
import time
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os
import urllib.request
import FinanceDataReader as fdr        # ← NEW

# ───────────────────────────────────────────────
# 0. 폰트 다운로드 (GitHub → 로컬)
# ───────────────────────────────────────────────
FONT_URL  = 'https://raw.githubusercontent.com/yongdragonee/semicon/main/NanumGothicCoding.ttf'
FONT_PATH = './NanumGothicCoding.ttf'
if not os.path.exists(FONT_PATH):
    urllib.request.urlretrieve(FONT_URL, FONT_PATH)
fontprop = fm.FontProperties(fname=FONT_PATH)

# ───────────────────────────────────────────────
# 1. CSV 로드 함수 (뉴스 / 보고서)
# ───────────────────────────────────────────────
def load_news_data(csv_url: str) -> pd.DataFrame:
    df = pd.read_csv(csv_url, encoding='utf-8-sig')
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.sort_values('date', ascending=False)
    if '키워드' in df.columns:
        df['키워드_목록'] = (df['키워드']
                             .fillna('')
                             .apply(lambda s: [k.strip() for k in s.split(',') if k.strip()]))
        df = df.explode('키워드_목록', ignore_index=True)
        df['키워드_목록'] = df['키워드_목록'].replace('관련 없음', '기타')
    return df


def load_report_data(csv_url: str) -> pd.DataFrame:
    try:
        return pd.read_csv(csv_url, encoding='utf-8-sig')
    except Exception as e:
        st.error(f"보고서 데이터를 불러오는 중 오류 발생: {e}")
        return pd.DataFrame()

# ───────────────────────────────────────────────
# 2. 주가 다운로드 (캐싱 + 재시도)
# ───────────────────────────────────────────────
@st.cache_data(ttl=900, show_spinner=False)
def download_prices(tickers: list[str],
                    start: datetime.date,
                    end: datetime.date,
                    retries: int = 2) -> pd.DataFrame:
    """
    FinanceDataReader를 이용해 종가만 가져와 하나의 DataFrame으로 합친다.
    (단일 API 장애 시를 대비해 간단히 재시도)
    """
    start_str, end_str = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    frames = []

    for t in tickers:
        for attempt in range(retries + 1):
            try:
                df = fdr.DataReader(t, start_str, end_str)[['Close']].rename(columns={'Close': t})
                frames.append(df)
                break
            except Exception as e:
                if attempt < retries:
                    time.sleep(5)
                else:
                    st.warning(f"{t} 데이터 수집 실패: {e}")

    return pd.concat(frames, axis=1) if frames else pd.DataFrame()

# ───────────────────────────────────────────────
# 3. 데이터 로드
# ───────────────────────────────────────────────
GITHUB_CSV_URL    = st.secrets["CSV_URL"]    + f"?nocache={int(time.time())}"
GITHUB_REPORT_URL = st.secrets["REPORT_URL"] + f"?nocache={int(time.time())}"

news_df   = load_news_data(GITHUB_CSV_URL)
report_df = load_report_data(GITHUB_REPORT_URL)

# 날짜 범위 기본값 잡기
if not news_df.empty and 'date' in news_df.columns:
    max_date       = news_df['date'].max()
    one_week_ago   = max_date - datetime.timedelta(days=7)
    one_month_ago  = max_date - datetime.timedelta(days=30)
else:
    one_week_ago = one_month_ago = None

# ───────────────────────────────────────────────
# 4. 화면 구성
# ───────────────────────────────────────────────

st.subheader("📢 반도체 뉴스레터 (Rev.25.3.29)")

# ---- 사이드바: 날짜 / 키워드 / 검색 ----
date_filter_option = st.sidebar.radio(
    "📅 날짜 필터 옵션",
    ["최근 7일", "최근 1달", "전체", "직접 선택"],
    index=0
)
if "date" in news_df.columns:
    unique_dates = sorted(news_df['date'].dt.date.dropna().unique(), reverse=True)
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
else:
    selected_dates = None

unique_keywords  = sorted(news_df['키워드_목록'].dropna().unique()) if '키워드_목록' in news_df.columns else []
selected_keywords = st.sidebar.multiselect("🔍 키워드 (복수 선택 가능)", unique_keywords)
search_query      = st.sidebar.text_input("🔎 검색어 입력 (제목/요약 포함)")
st.sidebar.write("---")
st.sidebar.write("문의/아이디어 : yh9003.lee@samsung.com")
st.sidebar.write("3/5 Streamlit 오픈 · 3/13 검색 추가 · 3/29 주가현황 추가 · 4/12 증권레포트 추가")

# ───────────────────────────────────────────────
# 5. 뉴스 필터링
# ───────────────────────────────────────────────
filtered_df = news_df.copy()
if selected_dates and 'date' in filtered_df.columns:
    filtered_df = filtered_df[filtered_df['date'].dt.date.isin(selected_dates)]
if selected_keywords:
    filtered_df = filtered_df[filtered_df['키워드_목록'].isin(selected_keywords)]
if search_query:
    q = search_query.lower()
    filtered_df = filtered_df[
        filtered_df['title'].str.lower().str.contains(q, na=False) |
        filtered_df['summary'].fillna('').str.lower().str.contains(q, na=False)
    ]
st.write(f"**총 기사 수:** {len(filtered_df)}개")

# ───────────────────────────────────────────────
# 6. 증권 레포트 (Expander)
# ───────────────────────────────────────────────
with st.expander("📊 증권 레포트", expanded=False):
    st.markdown("[https://semi-invest.streamlit.app/](https://semi-invest.streamlit.app/)")
    if not report_df.empty:
        st.dataframe(report_df)
    else:
        st.write("보고서 데이터를 불러올 수 없습니다.")

# ───────────────────────────────────────────────
# 7. 주가 정보 조회
# ───────────────────────────────────────────────
st.subheader("📈 주가 현황")

today = datetime.date.today()
start_date = today - datetime.timedelta(days=370)
end_date   = today + datetime.timedelta(days=1)

close_left  = pd.DataFrame()
close_right = pd.DataFrame()

col1, col2 = st.columns(2)

# ---- (2-A) 코스피/코스닥 ----
with col1:
    left_tickers = {"코스피": "KS11", "코스닥": "KQ11"}
    left_data   = download_prices(list(left_tickers.values()), start_date, end_date)
    if left_data.empty:
        st.warning("코스피/코스닥 데이터를 가져오지 못했습니다.")
    else:
        close_left = left_data.rename(columns={v: k for k, v in left_tickers.items()})

        fig, ax1 = plt.subplots(figsize=(8, 4))
        ax1.plot(close_left.index, close_left["코스피"],  label="코스피",  color='blue')
        ax1.set_ylabel("코스피",  color='blue', fontproperties=fontprop)
        ax1.tick_params(axis='y', labelcolor='blue')

        ax2 = ax1.twinx()
        ax2.plot(close_left.index, close_left["코스닥"], label="코스닥", color='red')
        ax2.set_ylabel("코스닥", color='red',  fontproperties=fontprop)
        ax2.tick_params(axis='y', labelcolor='red')

        # ★ 두 축의 범례 대상 합치기
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, prop=fontprop, loc="upper left")

        ax1.set_title("코스피 / 코스닥 지수", fontproperties=fontprop)
        st.pyplot(fig)

# ---- (2-B) 삼성전자/하이닉스 ----
with col2:
    right_tickers = {"삼성전자": "005930", "SK하이닉스": "000660"}
    right_data    = download_prices(list(right_tickers.values()), start_date, end_date)
    if right_data.empty:
        st.warning("삼성전자/SK하이닉스 데이터를 가져오지 못했습니다.")
    else:
        close_right = right_data.rename(columns={v: k for k, v in right_tickers.items()})

        fig, ax1 = plt.subplots(figsize=(8, 4))
        ax1.plot(close_right.index, close_right["삼성전자"],  label="삼성전자",  color='blue')
        ax1.set_ylabel("삼성전자",  color='blue', fontproperties=fontprop)
        ax1.tick_params(axis='y', labelcolor='blue')

        ax2 = ax1.twinx()
        ax2.plot(close_right.index, close_right["SK하이닉스"], label="SK하이닉스", color='red')
        ax2.set_ylabel("SK하이닉스", color='red', fontproperties=fontprop)
        ax2.tick_params(axis='y', labelcolor='red')

        # ★ 두 축 범례 합치기
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, prop=fontprop, loc="upper left")

        ax1.set_title("삼성전자 / SK하이닉스", fontproperties=fontprop)
        st.pyplot(fig)
# ---- 1일 변동률 간단 히든 요약 ----
if not close_right.empty:
    for ticker in ["삼성전자", "SK하이닉스"]:
        s = close_right[ticker].dropna().sort_index()
        if len(s) >= 2:
            pct = (s.iloc[-1] / s.iloc[-2] - 1) * 100
            st.write(f"{s.index[-1].date()} | {ticker}: {s.iloc[-1]:,.0f}원 / {pct:+.1f}%")
else:
    st.write("삼성전자/SK하이닉스 데이터가 없습니다.")

# ---- 변동률 표 + 정규화 그래프 ----
with st.expander("📑 주가 변동률 표 보기", expanded=False):

    # 국내 (코스피/코스닥 + 삼성전자/하이닉스)
    domestic_dfs = []
    for df in [close_left, close_right]:
        if df.empty:
            continue
        for col in df.columns:
            s = df[col].dropna().sort_index()
            if s.empty:
                continue
            latest = s.index[-1]
            row = {
                "종목": col,
                latest.strftime("%m/%d"): f"{s.iloc[-1]:,.0f}"
            }
            for days, tag in [(1, "-1D"), (7, "-1W"), (30, "-1M"), (365, "-1Y")]:
                prev = s[s.index <= latest - pd.Timedelta(days=days)]
                row[tag] = f"{(s.iloc[-1] / prev.iloc[-1] - 1) * 100:+.1f}%" if not prev.empty else "—"
            domestic_dfs.append(row)
    if domestic_dfs:
        st.markdown("# 🦖 국내")
        st.dataframe(pd.DataFrame(domestic_dfs).style.set_properties(**{"font-size": "11px"}))
    else:
        st.write("국내 데이터가 없습니다.")

    # 해외 (나스닥/필라델피아/마이크론)
    extra_tickers = {"나스닥": "IXIC", "필라델피아": "SOXX", "마이크론": "MU"}
    extra_data    = download_prices(list(extra_tickers.values()), start_date, end_date)
    foreign_dfs   = []
    if not extra_data.empty:
        close_extra = extra_data.rename(columns={v: k for k, v in extra_tickers.items()})
        for col in close_extra.columns:
            s = close_extra[col].dropna().sort_index()
            latest = s.index[-1]
            row = {
                "종목": col,
                latest.strftime("%m/%d"): f"{s.iloc[-1]:,.0f}"
            }
            for days, tag in [(1, "-1D"), (7, "-1W"), (30, "-1M"), (365, "-1Y")]:
                prev = s[s.index <= latest - pd.Timedelta(days=days)]
                row[tag] = f"{(s.iloc[-1] / prev.iloc[-1] - 1) * 100:+.1f}%" if not prev.empty else "—"
            foreign_dfs.append(row)
        st.markdown("# 🌏 해외")
        st.dataframe(pd.DataFrame(foreign_dfs).style.set_properties(**{"font-size": "11px"}))
    else:
        st.write("해외 데이터가 없습니다.")

    # ---- 1년 정규화 그래프 (코스닥 제외) ----
    if not close_left.empty or not close_right.empty or (locals().get("close_extra", pd.DataFrame()).empty is False):
        close_left_wo_kq = close_left.drop(columns=["코스닥"], errors="ignore")
        all_data = pd.concat([close_left_wo_kq, close_right, locals().get("close_extra", pd.DataFrame())], axis=1)
        all_data = all_data.sort_index()

        end_date  = all_data.dropna(how="all").index.max()
        base_date = end_date - pd.Timedelta(days=365)

        norm_dfs, valid_cols = [], []
        for col in all_data.columns:
            s = all_data[col].dropna().sort_index()
            base = s[s.index <= base_date]
            if base.empty:
                continue
            norm = (s / base.iloc[-1]) * 100
            norm_dfs.append(norm)
            valid_cols.append(col)

        if norm_dfs:
            normalized_all = pd.concat(norm_dfs, axis=1)
            normalized_all.columns = valid_cols
            normalized_all = normalized_all[normalized_all.index >= base_date]

            fig, ax = plt.subplots(figsize=(10, 5))
            for col in normalized_all.columns:
                ls = "--" if col in ["코스피", "나스닥", "필라델피아"] else "-"
                ax.plot(normalized_all.index, normalized_all[col], label=col, linestyle=ls)
            ax.set_ylabel("정규화 가격 (1년 전 = 100)", fontproperties=fontprop)
            ax.set_title("전체 정규화 가격 비교", fontproperties=fontprop)
            ax.legend(prop=fontprop)
            st.pyplot(fig)
        else:
            st.warning("1년 전 기준 데이터가 부족해 정규화 그래프를 그릴 수 없습니다.")
    else:
        st.warning("주가 데이터가 모두 비어 있어 정규화 그래프를 그릴 수 없습니다.")


# ───────────────────────────────────────────────
# 8. 뉴스 출력
# ───────────────────────────────────────────────
if "date" in filtered_df.columns:
    for date_key, date_group in filtered_df.groupby(filtered_df['date'].dt.date, sort=False):
        st.markdown(f"## {date_key.strftime('%Y-%m-%d')}")
        for kw, kw_group in date_group.groupby('키워드_목록', sort=False):
            st.markdown(f"### ▶️ {kw if pd.notna(kw) and str(kw).strip() else '(키워드 없음)'}")
            for _, row in kw_group.iterrows():
                with st.expander(f"📰 {row['title']}"):
                    st.write(f"**요약:** {row.get('summary', '요약 정보가 없습니다.')}")
                    link = row.get('link')
                    st.markdown(f"[🔗 기사 링크]({link})" if pd.notna(link) else "링크가 없습니다.")
else:
    st.write("표시할 뉴스 데이터가 없습니다.")
