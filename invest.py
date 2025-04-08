import streamlit as st
import pandas as pd
import time

# GitHub에 저장된 데이터 소스를 시크릿에서 불러오기
GITHUB_REPORT_URL = st.secrets["REPORT_URL"] + f"?nocache={int(time.time())}"

# 데이터 로딩 함수 (Streamlit 캐싱 이용)
@st.cache_data
def load_report_data(url):
    df = pd.read_csv(url)
    # 'date' 컬럼이 있다면 datetime 형식으로 변환
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df

# 데이터 불러오기
data = load_report_data(GITHUB_REPORT_URL)

# 사이드바 - 필터 옵션 (필요에 따라 추가)
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
    # 레포트 목록: 각 레포트의 날짜와 제목을 표시하는 선택 옵션 생성
    def format_report(idx):
        # 날짜가 없으면 'No Date' 표시
        date_str = data.loc[idx, 'date'].strftime('%Y-%m-%d') if pd.notnull(data.loc[idx, 'date']) else "No Date"
        return f"{date_str} - {data.loc[idx, 'title']}"

    # st.selectbox 옵션에는 데이터프레임의 인덱스 리스트를 사용하고, format_func로 날짜-제목 문자열 반환
    selected_idx = st.selectbox("레포트 선택", data.index.tolist(), format_func=format_report)

    # 선택된 레포트 세부 정보 출력
    report = data.loc[selected_idx]
    
    st.subheader(report.get('title', '제목 없음'))
    if 'date' in report:
        st.text("날짜: " + str(report['date'].date()))
    if 'analyst' in report:
        st.text("애널리스트: " + str(report['analyst']))
    if 'sector' in report:
        st.text("업종: " + str(report['sector']))

    # 요약 표시 (summary 컬럼이 있는 경우)
    if 'summary' in report:
        st.write("**요약**")
        st.write(report['summary'])
    else:
        st.write("요약 정보가 없습니다.")

    # 다운로드 버튼: 선택한 레포트의 데이터를 CSV 형식으로 다운로드
    # 버튼 레이블에 아이콘(📥)을 추가하였습니다.
    csv_data = report.to_csv(index=False)
    st.download_button(label="📥 다운로드", data=csv_data, file_name=f"{report.get('title', 'report')}.csv", mime="text/csv")
