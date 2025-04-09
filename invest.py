import streamlit as st
import pandas as pd
import time

# GitHub에서 데이터 불러오기
GITHUB_REPORT_URL = st.secrets["REPORT_URL"] + f"?nocache={int(time.time())}"

@st.cache_data
def load_report_data(url):
    df = pd.read_csv(url, encoding='utf-8-sig')
    if "날짜" in df.columns:
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
    return df

# 데이터 로드
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

# 사이드바 필터
st.sidebar.header("필터 옵션")

# 날짜 필터
if DATE_COLUMN in data.columns:
    min_date = data[DATE_COLUMN].min()
    max_date = data[DATE_COLUMN].max()
    date_range = st.sidebar.date_input("날짜 범위 선택", [min_date, max_date])
    if len(date_range) == 2:
        start_date, end_date = date_range
        data = data[(data[DATE_COLUMN] >= pd.Timestamp(start_date)) & (data[DATE_COLUMN] <= pd.Timestamp(end_date))]

# 증권사 필터
if ANALYST_COLUMN in data.columns:
    securities = data[ANALYST_COLUMN].unique().tolist()
    selected_securities = st.sidebar.multiselect("증권사 선택", securities, default=securities)
    data = data[data[ANALYST_COLUMN].isin(selected_securities)]

# 키워드 검색
keyword = st.sidebar.text_input("키워드 검색 (레포트제목, 전체요약)")
if keyword:
    condition = pd.Series(False, index=data.index)
    if TITLE_COLUMN in data.columns:
        condition |= data[TITLE_COLUMN].str.contains(keyword, case=False, na=False)
    if SUMMARY_COLUMN in data.columns:
        condition |= data[SUMMARY_COLUMN].str.contains(keyword, case=False, na=False)
    data = data[condition]

# 헤더
st.title("📊 증권 레포트 모음")
st.write("아래 목록에서 제목을 눌러 세부 내용을 확인하세요.")

# HTML로 줄바꿈 포함한 요약 출력 함수
def format_report(idx):
    row = data.loc[idx]

    # 날짜
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

    # HTML summary
    html = f"""
    <details>
      <summary><b>{date_str} - {title_str}</b></summary>
      <div style='margin-top: 10px; padding-left: 10px;'>
        <p>📌 <b>1줄 요약:</b><br>{one_line_summary}</p>
        <p>🔑 <b>키워드:</b><br>{keywords}</p>
      </div>
    </details>
    """
    return html

# 데이터 출력
if data.empty:
    st.write("선택된 필터에 해당하는 레포트가 없습니다.")
else:
    for idx in data.index:
        # 줄바꿈 포함 미리보기 출력
        st.markdown(format_report(idx), unsafe_allow_html=True)

        # 상세 내용 표시
        report = data.loc[idx]

        # 전체요약
        st.write("**📝 전체요약**")
        st.write(report[SUMMARY_COLUMN] if pd.notnull(report[SUMMARY_COLUMN]) else "요약 정보가 없습니다.")

        # 증권사
        st.write("**🏢 증권사:**", report[ANALYST_COLUMN] if pd.notnull(report[ANALYST_COLUMN]) else "정보 없음")

        # 파일크기
        st.write("**📁 파일크기:**", report[FILESIZE_COLUMN] if pd.notnull(report[FILESIZE_COLUMN]) else "정보 없음")

        # 첨부파일 링크 버튼 (있을 경우에만)
        if LINK_COLUMN in report and pd.notnull(report[LINK_COLUMN]):
            st.markdown(
                f'<a href="{report[LINK_COLUMN]}" target="_blank"><button>📎 첨부파일 열기</button></a>',
                unsafe_allow_html=True
            )
        else:
            st.info("링크 정보가 없습니다.")

        # CSV 다운로드
        csv_data = report.to_csv(index=False)
        download_file_name = f"{report[TITLE_COLUMN] if pd.notnull(report[TITLE_COLUMN]) else 'report'}.csv"
        st.download_button(label="📥 다운로드", data=csv_data, file_name=download_file_name, mime="text/csv", key=f"download_{idx}")
        st.markdown("---")  # 구분선
