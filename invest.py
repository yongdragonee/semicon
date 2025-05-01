import streamlit as st
import pandas as pd
import time

# GitHub에 저장된 데이터 소스를 st.secrets에서 불러오며, 캐시 우회를 위해 현재 시간을 붙임
GITHUB_REPORT_URL = st.secrets["REPORT_URL"] + f"?nocache={int(time.time())}"

@st.cache_data
def load_report_data(url):
    # 한글이 올바르게 표시되도록 encoding 지정
    df = pd.read_csv(url, encoding='utf-8-sig')
    if "날짜" in df.columns:
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
    return df

# 데이터 불러오기 (CSV의 전체 9개 칼럼 포함)
data = load_report_data(GITHUB_REPORT_URL)

# 전체 9개 칼럼 정의
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

# 1. 날짜 필터링
if DATE_COLUMN in data.columns:
    min_date = data[DATE_COLUMN].min()
    max_date = data[DATE_COLUMN].max()
    date_range = st.sidebar.date_input("날짜 범위 선택", [min_date, max_date])
    if len(date_range) == 2:
        start_date, end_date = date_range
        data = data[(data[DATE_COLUMN] >= pd.Timestamp(start_date)) & (data[DATE_COLUMN] <= pd.Timestamp(end_date))]

# 2. 증권사 필터링
if ANALYST_COLUMN in data.columns:
    securities = data[ANALYST_COLUMN].unique().tolist()
    selected_securities = st.sidebar.multiselect("증권사 선택", securities, default=securities)
    data = data[data[ANALYST_COLUMN].isin(selected_securities)]

# 3. 키워드 검색 (레포트제목, 전체요약)
keyword = st.sidebar.text_input("키워드 검색 (레포트제목, 전체요약)")
if keyword:
    condition = pd.Series(False, index=data.index)
    if TITLE_COLUMN in data.columns:
        condition |= data[TITLE_COLUMN].str.contains(keyword, case=False, na=False)
    if SUMMARY_COLUMN in data.columns:
        condition |= data[SUMMARY_COLUMN].str.contains(keyword, case=False, na=False)
    data = data[condition]

st.title("📊 증권 레포트 모음(Ver.25.5.1)")
st.write("아래 목록에서 제목을 클릭하면 상세 내용이 펼쳐집니다.")

def format_report(idx):
    """
    HTML의 <details> 태그를 활용해
      - <summary> 안에는 날짜, 제목, 1줄 요약, 키워드를 모두 표시  
      - <div> 영역 안에 "# 데이터 출력" 헤더 아래 전체 상세 내용을 표현합니다.
    """
    row = data.loc[idx]

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

    # 상세 데이터 출력 (전체요약, 증권사, 파일크기, 첨부파일 링크)
    full_summary = row[SUMMARY_COLUMN].strip() if pd.notnull(row[SUMMARY_COLUMN]) else "요약 정보가 없습니다."
    analyst_info = str(row[ANALYST_COLUMN]) if pd.notnull(row[ANALYST_COLUMN]) else "정보 없음"
    filesize_info = str(row[FILESIZE_COLUMN]) if pd.notnull(row[FILESIZE_COLUMN]) else "정보 없음"

    # 첨부파일 링크 (있을 경우)
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

if data.empty:
    st.write("선택된 필터에 해당하는 레포트가 없습니다.")
else:
    # 각 레포트를 HTML로 출력
    for idx in data.index:
        st.markdown(format_report(idx), unsafe_allow_html=True)
        
        # CSV 다운로드 버튼 (HTML 영역과 별개로 인터랙티브하게 구성)
        #report = data.loc[idx]
        #csv_data = report.to_csv(index=False)
        #download_file_name = f"{report[TITLE_COLUMN] if pd.notnull(report[TITLE_COLUMN]) else 'report'}.csv"
        #st.download_button(label="📥 다운로드", data=csv_data, file_name=download_file_name, mime="text/csv", key=f"download_{idx}")
        #st.markdown("---")
