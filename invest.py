import streamlit as st
import pandas as pd
import time

# GitHub에 저장된 데이터 소스를 st.secrets에서 불러오며, 캐시 우회를 위해 현재 시간을 붙임
GITHUB_REPORT_URL = st.secrets["REPORT_URL"] + f"?nocache={int(time.time())}"

# 데이터 로딩 함수 (Streamlit 캐싱 사용)
@st.cache_data
def load_report_data(url):
    df = pd.read_csv(url)
    # 날짜 컬럼이 "date" 또는 "날짜"에 해당하면 datetime 형식으로 변환
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    elif "날짜" in df.columns:
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
    return df

# 데이터 불러오기
data = load_report_data(GITHUB_REPORT_URL)

# CSV 파일 컬럼명에 따라 사용될 컬럼 이름 설정 (영어 우선, 없으면 한글)
TITLE_COLUMN = "title" if "title" in data.columns else ("제목" if "제목" in data.columns else None)
DATE_COLUMN = "date" if "date" in data.columns else ("날짜" if "날짜" in data.columns else None)
SUMMARY_COLUMN = "summary" if "summary" in data.columns else ("요약" if "요약" in data.columns else None)
ANALYST_COLUMN = "analyst" if "analyst" in data.columns else ("애널리스트" if "애널리스트" in data.columns else None)
SECTOR_COLUMN = "sector" if "sector" in data.columns else ("업종" if "업종" in data.columns else None)

# 사이드바 - 필터 옵션 (필요시 추가)
st.sidebar.header("필터 옵션")
if DATE_COLUMN and DATE_COLUMN in data.columns:
    min_date = data[DATE_COLUMN].min()
    max_date = data[DATE_COLUMN].max()
    date_range = st.sidebar.date_input("날짜 범위 선택", [min_date, max_date])
    if len(date_range) == 2:
        start_date, end_date = date_range
        data = data[(data[DATE_COLUMN] >= pd.Timestamp(start_date)) & (data[DATE_COLUMN] <= pd.Timestamp(end_date))]

if SECTOR_COLUMN and SECTOR_COLUMN in data.columns:
    sectors = data[SECTOR_COLUMN].unique().tolist()
    selected_sectors = st.sidebar.multiselect("업종 선택", sectors, default=sectors)
    data = data[data[SECTOR_COLUMN].isin(selected_sectors)]

# 키워드 검색 (제목, 요약 컬럼이 존재하는 경우에만)
keyword = st.sidebar.text_input("키워드 검색 (제목, 요약)")
if keyword:
    condition = pd.Series(False, index=data.index)
    if TITLE_COLUMN and TITLE_COLUMN in data.columns:
        condition |= data[TITLE_COLUMN].str.contains(keyword, case=False, na=False)
    if SUMMARY_COLUMN and SUMMARY_COLUMN in data.columns:
        condition |= data[SUMMARY_COLUMN].str.contains(keyword, case=False, na=False)
    data = data[condition]

st.title("증권 레포트 모음")
st.write("좌측 목록에서 레포트를 선택하세요.")

if data.empty:
    st.write("선택된 필터에 해당하는 레포트가 없습니다.")
else:
    # 레포트 목록 생성: 각 레포트의 날짜와 제목을 "날짜 - 제목" 형식으로 반환합니다.
    def format_report(idx):
        row = data.loc[idx]
        # 날짜 처리: DATE_COLUMN이 있고 값이 있으면 형식화, 없으면 "No Date"
        if DATE_COLUMN and DATE_COLUMN in row and pd.notnull(row[DATE_COLUMN]):
            date_val = row[DATE_COLUMN]
            date_str = date_val.strftime('%Y-%m-%d') if isinstance(date_val, pd.Timestamp) else str(date_val)
        else:
            date_str = "No Date"
        # 제목 처리: TITLE_COLUMN이 있고 값이 있으면 사용, 없으면 "제목 없음"
        title_str = row[TITLE_COLUMN] if TITLE_COLUMN and TITLE_COLUMN in row and pd.notnull(row[TITLE_COLUMN]) else "제목 없음"
        return f"{date_str} - {title_str}"

    # st.selectbox에 데이터프레임의 인덱스 목록을 넣고, format_func를 이용하여 표시 문자열을 생성합니다.
    selected_idx = st.selectbox("레포트 선택", data.index.tolist(), format_func=format_report)

    # 선택한 레포트에 대한 세부 정보 출력
    report = data.loc[selected_idx]
    
    st.subheader(report[TITLE_COLUMN] if TITLE_COLUMN and TITLE_COLUMN in report else "제목 없음")
    if DATE_COLUMN and DATE_COLUMN in report:
        if pd.notnull(report[DATE_COLUMN]):
            date_text = report[DATE_COLUMN].strftime('%Y-%m-%d') if isinstance(report[DATE_COLUMN], pd.Timestamp) else str(report[DATE_COLUMN])
        else:
            date_text = "No Date"
        st.text("날짜: " + date_text)
    if ANALYST_COLUMN and ANALYST_COLUMN in report:
        st.text("애널리스트: " + str(report[ANALYST_COLUMN]))
    if SECTOR_COLUMN and SECTOR_COLUMN in report:
        st.text("업종: " + str(report[SECTOR_COLUMN]))

    if SUMMARY_COLUMN and SUMMARY_COLUMN in report:
        st.write("**요약**")
        st.write(report[SUMMARY_COLUMN])
    else:
        st.write("요약 정보가 없습니다.")

    # 다운로드 버튼: 선택된 레포트를 CSV 형식으로 다운로드 (아이콘 포함)
    csv_data = report.to_csv(index=False)
    download_file_name = f"{report[TITLE_COLUMN] if TITLE_COLUMN and TITLE_COLUMN in report else 'report'}.csv"
    st.download_button(label="📥 다운로드", data=csv_data, file_name=download_file_name, mime="text/csv")
