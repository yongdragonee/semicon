import streamlit as st
import pandas as pd
import time

# GitHub에 저장된 데이터 소스를 st.secrets에서 불러오며, 캐시 우회를 위해 현재 시간을 붙임
GITHUB_REPORT_URL = st.secrets["REPORT_URL"] + f"?nocache={int(time.time())}"

# 데이터 로딩 함수 (Streamlit 캐싱 사용)
@st.cache_data
def load_report_data(url):
    df = pd.read_csv(url)
    # 'date' 컬럼이 있으면 datetime 형식으로 변환
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df

# 데이터 불러오기
data = load_report_data(GITHUB_REPORT_URL)

# 사이드바 - 필터 옵션 (필요시 추가)
st.sidebar.header("필터 옵션")
if 'date' in data.columns:
    min_date = data['date'].min()
    max_date = data['date'].max()
    date_range = st.sidebar.date_input("날짜 범위 선택", [min_date, max_date])
    if len(date_range) == 2:
        start_date, end_date = date_range
        data = data[(data['date'] >= pd.Timestamp(start_date)) & (data['date'] <= pd.Timestamp(end_date))]

if 'sector' in data.columns:
    sectors = data['sector'].unique().tolist()
    selected_sectors = st.sidebar.multiselect("업종 선택", sectors, default=sectors)
    data = data[data['sector'].isin(selected_sectors)]

keyword = st.sidebar.text_input("키워드 검색 (제목, 요약)")
if keyword:
    data = data[
        data['title'].str.contains(keyword, case=False, na=False) |
        data['summary'].str.contains(keyword, case=False, na=False)
    ]

st.title("증권 레포트 모음")
st.write("좌측 목록에서 레포트를 선택하세요.")

if data.empty:
    st.write("선택된 필터에 해당하는 레포트가 없습니다.")
else:
    # 레포트 목록 생성: 각 레포트의 날짜와 제목을 "날짜 - 제목" 형식으로 반환합니다.
    def format_report(idx):
        row = data.loc[idx]
        # 'date' 컬럼이 존재하고 값이 있으면 날짜 문자열 생성, 없으면 "No Date"
        if 'date' in data.columns and pd.notnull(row['date']):
            date_str = row['date'].strftime('%Y-%m-%d')
        else:
            date_str = "No Date"
        # 'title' 컬럼이 존재하면 제목 사용, 없으면 "제목 없음"
        title_str = row['title'] if 'title' in data.columns else "제목 없음"
        return f"{date_str} - {title_str}"

    # st.selectbox에 데이터프레임의 인덱스 목록을 넣고, format_func를 이용하여 표시 문자열 생성
    selected_idx = st.selectbox("레포트 선택", data.index.tolist(), format_func=format_report)

    # 선택된 레포트 세부 정보 표시
    report = data.loc[selected_idx]
    
    st.subheader(report.get('title', '제목 없음'))
    if 'date' in report:
        date_text = report['date'].strftime('%Y-%m-%d') if pd.notnull(report['date']) else "No Date"
        st.text("날짜: " + date_text)
    if 'analyst' in report:
        st.text("애널리스트: " + str(report['analyst']))
    if 'sector' in report:
        st.text("업종: " + str(report['sector']))

    # 요약 내용 출력 (summary 컬럼이 있을 경우)
    if 'summary' in report:
        st.write("**요약**")
        st.write(report['summary'])
    else:
        st.write("요약 정보가 없습니다.")

    # 다운로드 버튼: 선택된 레포트를 CSV 형식으로 다운로드 (버튼 레이블에 아이콘 포함)
    csv_data = report.to_csv(index=False)
    st.download_button(label="📥 다운로드", data=csv_data, file_name=f"{report.get('title', 'report')}.csv", mime="text/csv")
