import streamlit as st
import pandas as pd
import time

# 캐시 우회를 제거하거나, 하루 단위로만 변경되도록 수정
GITHUB_REPORT_URL = st.secrets["REPORT_URL"]

@st.cache_data(ttl=3600)  # 1시간 캐시 (필요시 조정)
def load_report_data(url):
    df = pd.read_csv(url, encoding='utf-8-sig')
    if "날짜" in df.columns:
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
    return df

# 데이터 불러오기
data = load_report_data(GITHUB_REPORT_URL)

# 칼럼 정의
DATE_COLUMN = "날짜"
ANALYST_COLUMN = "증권사"
TITLE_COLUMN = "레포트제목"
FULL_TEXT_COLUMN = "레포트본문전체"
SUMMARY_COLUMN = "전체요약"
SUMMARY_COLUMN_1LINE = "1줄 요약"
KEYWORDS_COLUMN = "키워드"
LINK_COLUMN = "link"
FILESIZE_COLUMN = "파일크기"

# 사이드바 - 필터 옵션
st.sidebar.header("필터 옵션")

# 필터링된 데이터를 별도 변수로 관리
filtered_data = data.copy()

# 1. 날짜 필터링
if DATE_COLUMN in filtered_data.columns:
    min_date = filtered_data[DATE_COLUMN].min()
    max_date = filtered_data[DATE_COLUMN].max()
    date_range = st.sidebar.date_input("날짜 범위 선택", [min_date, max_date])
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_data = filtered_data[
            (filtered_data[DATE_COLUMN] >= pd.Timestamp(start_date)) & 
            (filtered_data[DATE_COLUMN] <= pd.Timestamp(end_date))
        ]

# 2. 증권사 필터링
if ANALYST_COLUMN in filtered_data.columns:
    securities = filtered_data[ANALYST_COLUMN].unique().tolist()
    selected_securities = st.sidebar.multiselect("증권사 선택", securities, default=securities)
    filtered_data = filtered_data[filtered_data[ANALYST_COLUMN].isin(selected_securities)]

# 3. 키워드 검색
keyword = st.sidebar.text_input("키워드 검색 (레포트제목, 전체요약)")
if keyword:
    condition = pd.Series(False, index=filtered_data.index)
    if TITLE_COLUMN in filtered_data.columns:
        condition |= filtered_data[TITLE_COLUMN].str.contains(keyword, case=False, na=False)
    if SUMMARY_COLUMN in filtered_data.columns:
        condition |= filtered_data[SUMMARY_COLUMN].str.contains(keyword, case=False, na=False)
    filtered_data = filtered_data[condition]

st.title("📊 증권 레포트 모음(Ver.25.5.1)")

# 📌 페이지네이션 추가
ITEMS_PER_PAGE = 20  # 페이지당 표시할 레포트 수

if not filtered_data.empty:
    total_items = len(filtered_data)
    total_pages = (total_items - 1) // ITEMS_PER_PAGE + 1
    
    # 페이지 선택
    page = st.sidebar.number_input(
        f"페이지 (총 {total_pages}페이지, {total_items}개 레포트)",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1
    )
    
    # 현재 페이지의 데이터만 가져오기
    start_idx = (page - 1) * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    page_data = filtered_data.iloc[start_idx:end_idx]
    
    st.write(f"**{start_idx + 1}~{min(end_idx, total_items)}번째 레포트 표시 중** (전체 {total_items}개)")
else:
    st.write("선택된 필터에 해당하는 레포트가 없습니다.")
    page_data = pd.DataFrame()

def format_report(row):
    """HTML 포맷팅 - 인덱스 대신 row를 직접 받음"""
    # 날짜 포맷팅
    if DATE_COLUMN in row and pd.notnull(row[DATE_COLUMN]):
        date_val = row[DATE_COLUMN]
        date_str = date_val.strftime('%Y-%m-%d') if isinstance(date_val, pd.Timestamp) else str(date_val)
    else:
        date_str = "날짜 없음"

    # 제목
    title_str = row[TITLE_COLUMN] if pd.notnull(row[TITLE_COLUMN]) else "제목 없음"

    # 1줄 요약
    one_line_summary = row[SUMMARY_COLUMN_1LINE].strip() if pd.notnull(row[SUMMARY_COLUMN_1LINE]) else "요약 없음"

    # 키워드
    keywords = row[KEYWORDS_COLUMN].strip() if pd.notnull(row[KEYWORDS_COLUMN]) else "키워드 없음"

    # 상세 데이터
    full_summary = row[SUMMARY_COLUMN].strip() if pd.notnull(row[SUMMARY_COLUMN]) else "요약 정보가 없습니다."
    analyst_info = str(row[ANALYST_COLUMN]) if pd.notnull(row[ANALYST_COLUMN]) else "정보 없음"
    filesize_info = str(row[FILESIZE_COLUMN]) if pd.notnull(row[FILESIZE_COLUMN]) else "정보 없음"

    # 첨부파일 링크
    if LINK_COLUMN in row and pd.notnull(row[LINK_COLUMN]):
        link_html = f'<p><a href="{row[LINK_COLUMN]}" target="_blank"><button>📎 첨부파일 열기</button></a></p>'
    else:
        link_html = "<p>첨부파일 링크가 없습니다.</p>"

    html = f"""
    <details>
        <summary>
          <span style="font-size: 20px;"><b>{date_str} - {title_str}</b></span><br>
          <span style="font-size: 17px;">
            📌 {one_line_summary}<br> </span>
          <span style="font-size: 14px;"> 
            🔑 {keywords}<br>
            💾 {filesize_info} Byte<br><br>
          </span>
        </summary>
      <div style='margin-top: 11px; padding-left: 11px;'>
        <p>
         <b>전체요약:</b><br>
         {full_summary}
        </p>
        <p><b>증권사:</b> {analyst_info}</p>
        <p><b>파일크기:</b> {filesize_info}</p>
        {link_html}
      </div>
    </details>
    """
    return html

# 페이지 데이터 출력
if not page_data.empty:
    for idx, row in page_data.iterrows():
        st.markdown(format_report(row), unsafe_allow_html=True)
