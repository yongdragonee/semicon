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

# 데이터 불러오기
data = load_report_data(GITHUB_REPORT_URL)

# 컬럼명 설정 (제공된 칼럼 기준)
DATE_COLUMN = "날짜"
TITLE_COLUMN = "레포트제목"
SUMMARY_COLUMN = "전체요약"
ANALYST_COLUMN = "증권사"

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
if "증권사" in data.columns:
    securities = data["증권사"].unique().tolist()
    selected_securities = st.sidebar.multiselect("증권사 선택", securities, default=securities)
    data = data[data["증권사"].isin(selected_securities)]

# 3. 키워드 검색 (레포트제목, 전체요약)
keyword = st.sidebar.text_input("키워드 검색 (레포트제목, 전체요약)")
if keyword:
    condition = pd.Series(False, index=data.index)
    if TITLE_COLUMN in data.columns:
        condition |= data[TITLE_COLUMN].str.contains(keyword, case=False, na=False)
    if SUMMARY_COLUMN in data.columns:
        condition |= data[SUMMARY_COLUMN].str.contains(keyword, case=False, na=False)
    data = data[condition]

st.title("증권 레포트 모음")
st.write("아래 목록에서 레포트를 선택하세요.")

# 각 레포트의 날짜, 제목, 1줄 요약을 반환하는 함수
def format_report(idx):
    row = data.loc[idx]
    # 날짜 포맷팅
    if DATE_COLUMN in row and pd.notnull(row[DATE_COLUMN]):
        date_val = row[DATE_COLUMN]
        date_str = date_val.strftime('%Y-%m-%d') if isinstance(date_val, pd.Timestamp) else str(date_val)
    else:
        date_str = "날짜 없음"
    # 제목
    title_str = row[TITLE_COLUMN] if TITLE_COLUMN in row and pd.notnull(row[TITLE_COLUMN]) else "제목 없음"
    # 1줄 요약: 개행 문자로 나눈 첫 줄 사용, 길면 50자로 자름
    if SUMMARY_COLUMN in row and pd.notnull(row[SUMMARY_COLUMN]):
        summary_full = row[SUMMARY_COLUMN].strip()
        summary_line = summary_full.splitlines()[0]
        one_line_summary = summary_line if len(summary_line) < 50 else summary_line[:50] + "..."
    else:
        one_line_summary = "요약 없음"
    return f"{date_str} - {title_str} - {one_line_summary}"

# URL 쿼리 파라미터 확인하여 선택된 레포트가 있는지 확인
query_params = st.experimental_get_query_params()
selected_report_idx = query_params.get("report", [None])[0]

if selected_report_idx is None:
    # 레포트 목록을 Markdown 링크 형식으로 출력
    st.markdown("### 레포트 목록")
    for idx in data.index:
        line = format_report(idx)
        # 링크 클릭 시 URL 쿼리 파라미터에 report 항목으로 해당 인덱스를 추가하여 페이지 리로드
        link = f'<a href="?report={idx}">{line}</a>'
        st.markdown(link, unsafe_allow_html=True)
else:
    # 선택된 레포트 세부 정보 표시
    try:
        selected_idx = int(selected_report_idx)
    except ValueError:
        st.error("잘못된 보고서 선택입니다.")
    else:
        if selected_idx not in data.index:
            st.error("유효하지 않은 보고서입니다.")
        else:
            report = data.loc[selected_idx]
            st.subheader(report[TITLE_COLUMN] if TITLE_COLUMN in report and pd.notnull(report[TITLE_COLUMN]) else "제목 없음")
            if DATE_COLUMN in report:
                if pd.notnull(report[DATE_COLUMN]):
                    date_text = report[DATE_COLUMN].strftime('%Y-%m-%d') if isinstance(report[DATE_COLUMN], pd.Timestamp) else str(report[DATE_COLUMN])
                else:
                    date_text = "날짜 없음"
                st.text("날짜: " + date_text)
            if ANALYST_COLUMN in report:
                st.text("증권사: " + str(report[ANALYST_COLUMN]))
            
            if SUMMARY_COLUMN in report:
                st.write("**전체요약**")
                st.write(report[SUMMARY_COLUMN])
            else:
                st.write("요약 정보가 없습니다.")
            
            # 첨부파일 아이콘 버튼 (현재는 클릭 시 안내 메시지)
            if st.button("📎 첨부파일"):
                st.info("첨부파일 기능은 별도 구현이 필요합니다.")
            
            # 다운로드 버튼: 선택된 레포트를 CSV 형식으로 다운로드
            csv_data = report.to_csv(index=False)
            download_file_name = f"{report[TITLE_COLUMN] if TITLE_COLUMN in report and pd.notnull(report[TITLE_COLUMN]) else 'report'}.csv"
            st.download_button(label="📥 다운로드", data=csv_data, file_name=download_file_name, mime="text/csv")
            
            # 뒤로가기 버튼: 쿼리 파라미터 초기화 후 페이지 리로드
            if st.button("뒤로가기"):
                st.experimental_set_query_params()
                st.experimental_rerun()
