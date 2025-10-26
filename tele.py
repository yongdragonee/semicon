import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import html

# 페이지 설정
st.set_page_config(
    page_title="반도체 정보 수집기",
    page_icon="💬",
    layout="wide"
)

# GitHub CSV URL
GITHUB_TELE_URL = st.secrets["TELE_URL"]

# CSV 파일 로드 함수
@st.cache_data(ttl=3600)  # 1시간 캐시
def load_data(url):
    df = pd.read_csv(url, encoding='utf-8-sig')
    # 날짜 컬럼 변환
    if "date_local" in df.columns:
        df["date_local"] = pd.to_datetime(df["date_local"], errors="coerce")
    return df

# 데이터 불러오기
try:
    data = load_data(GITHUB_TELE_URL)
except Exception as e:
    st.error(f"CSV 파일을 불러오는 중 오류가 발생했습니다: {str(e)}")
    st.stop()

# 컬럼 정의
DATE_COLUMN = "date_local"
CHANNEL_COLUMN = "channel"
LABELS_COLUMN = "labels"
MESSAGE_COLUMN = "message"
NORMALIZED_TEXT_COLUMN = "normalized_text"
SUMMARY_COLUMN = "summary"
KEYWORDS_COLUMN = "keywords"
SENTIMENT_COLUMN = "sentiment"
MESSAGE_LENGTH_COLUMN = "message_length"

# 사이드바 - 필터 옵션
st.sidebar.header("🔍 필터 옵션")

# 필터링된 데이터 관리
filtered_data = data.copy()

# 1. 날짜 필터링
if DATE_COLUMN in filtered_data.columns and not filtered_data[DATE_COLUMN].isna().all():
    min_date = filtered_data[DATE_COLUMN].min()
    max_date = filtered_data[DATE_COLUMN].max()
    
    date_range = st.sidebar.date_input(
        "📅 날짜 범위 선택", 
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_data = filtered_data[
            (filtered_data[DATE_COLUMN] >= pd.Timestamp(start_date)) & 
            (filtered_data[DATE_COLUMN] <= pd.Timestamp(end_date))
        ]

# 2. 채널 필터링
if CHANNEL_COLUMN in filtered_data.columns:
    channels = sorted(filtered_data[CHANNEL_COLUMN].unique().tolist())
    selected_channels = st.sidebar.multiselect(
        "📺 채널 선택", 
        channels, 
        default=channels
    )
    filtered_data = filtered_data[filtered_data[CHANNEL_COLUMN].isin(selected_channels)]

# 3. 라벨 필터링
if LABELS_COLUMN in filtered_data.columns:
    # 세미콜론으로 구분된 라벨을 분리하여 고유 라벨 추출
    all_labels = set()
    for labels in filtered_data[LABELS_COLUMN].dropna():
        all_labels.update([label.strip() for label in str(labels).split(';')])
    
    all_labels = sorted(list(all_labels))
    selected_labels = st.sidebar.multiselect(
        "🏷️ 라벨 선택 (AND 조건)", 
        all_labels
    )
    
    if selected_labels:
        # 선택된 모든 라벨을 포함하는 메시지만 필터링
        def has_all_labels(labels_str):
            if pd.isna(labels_str):
                return False
            labels = [label.strip() for label in str(labels_str).split(';')]
            return all(selected_label in labels for selected_label in selected_labels)
        
        filtered_data = filtered_data[filtered_data[LABELS_COLUMN].apply(has_all_labels)]

# 4. 감성 분석 필터링
if SENTIMENT_COLUMN in filtered_data.columns:
    sentiments = sorted(filtered_data[SENTIMENT_COLUMN].dropna().unique().tolist())
    selected_sentiments = st.sidebar.multiselect(
        "😊 감성 선택", 
        sentiments, 
        default=sentiments
    )
    filtered_data = filtered_data[filtered_data[SENTIMENT_COLUMN].isin(selected_sentiments)]

# 5. 키워드 검색
keyword = st.sidebar.text_input("🔎 키워드 검색 (메시지, 요약, 키워드)")
if keyword:
    condition = pd.Series(False, index=filtered_data.index)
    if NORMALIZED_TEXT_COLUMN in filtered_data.columns:
        condition |= filtered_data[NORMALIZED_TEXT_COLUMN].str.contains(keyword, case=False, na=False)
    if SUMMARY_COLUMN in filtered_data.columns:
        condition |= filtered_data[SUMMARY_COLUMN].str.contains(keyword, case=False, na=False)
    if KEYWORDS_COLUMN in filtered_data.columns:
        condition |= filtered_data[KEYWORDS_COLUMN].str.contains(keyword, case=False, na=False)
    filtered_data = filtered_data[condition]

# 6. 메시지 길이 필터
if MESSAGE_LENGTH_COLUMN in filtered_data.columns:
    min_length = int(filtered_data[MESSAGE_LENGTH_COLUMN].min())
    max_length = int(filtered_data[MESSAGE_LENGTH_COLUMN].max())
    
    length_range = st.sidebar.slider(
        "📏 메시지 길이 범위",
        min_value=min_length,
        max_value=max_length,
        value=(min_length, max_length)
    )
    
    filtered_data = filtered_data[
        (filtered_data[MESSAGE_LENGTH_COLUMN] >= length_range[0]) &
        (filtered_data[MESSAGE_LENGTH_COLUMN] <= length_range[1])
    ]

# 메인 타이틀
st.title("💬 반도체 정보 수집기")

# 페이지네이션
ITEMS_PER_PAGE = 50

if not filtered_data.empty:
    # 날짜 내림차순 정렬
    filtered_data = filtered_data.sort_values(by=DATE_COLUMN, ascending=False)
    
    total_items = len(filtered_data)
    total_pages = (total_items - 1) // ITEMS_PER_PAGE + 1
    
    # 페이지 선택
    page = st.sidebar.number_input(
        f"📄 페이지 (총 {total_pages}페이지, {total_items}개 메시지)",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1
    )
    
    # 현재 페이지의 데이터
    start_idx = (page - 1) * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    page_data = filtered_data.iloc[start_idx:end_idx]
    
    st.write(f"**{start_idx + 1}~{min(end_idx, total_items)}번째 메시지 표시 중** (전체 {total_items}개)")
else:
    st.warning("선택된 필터에 해당하는 메시지가 없습니다.")
    page_data = pd.DataFrame()

# 메시지 포맷팅 함수
def format_message(row):
    """HTML 포맷팅"""
    # 날짜 포맷팅
    date_str = "날짜 없음"
    if DATE_COLUMN in row.index and pd.notnull(row[DATE_COLUMN]):
        date_val = row[DATE_COLUMN]
        if isinstance(date_val, pd.Timestamp):
            date_str = date_val.strftime('%Y-%m-%d %H:%M')
        else:
            date_str = str(date_val)
    date_str = html.escape(date_str)
    
    # 채널 정보
    channel_str = "채널 없음"
    if CHANNEL_COLUMN in row.index and pd.notnull(row[CHANNEL_COLUMN]):
        channel_str = str(row[CHANNEL_COLUMN])
    channel_str = html.escape(channel_str)
    
    # 라벨
    labels_str = "라벨 없음"
    if LABELS_COLUMN in row.index and pd.notnull(row[LABELS_COLUMN]):
        labels_str = str(row[LABELS_COLUMN])
    labels_str = html.escape(labels_str)
    
    # 감성
    sentiment_raw = "감성 없음"
    if SENTIMENT_COLUMN in row.index and pd.notnull(row[SENTIMENT_COLUMN]):
        sentiment_raw = str(row[SENTIMENT_COLUMN])
    sentiment_emoji = {"긍정적": "😊", "부정적": "😟", "중립적": "😐"}.get(sentiment_raw, "❓")
    sentiment_str = html.escape(sentiment_raw)
    
    # 키워드
    keywords_str = "키워드 없음"
    if KEYWORDS_COLUMN in row.index and pd.notnull(row[KEYWORDS_COLUMN]):
        keywords_str = str(row[KEYWORDS_COLUMN])
    keywords_str = html.escape(keywords_str)
    
    # 메시지 길이
    msg_length = "0"
    if MESSAGE_LENGTH_COLUMN in row.index and pd.notnull(row[MESSAGE_LENGTH_COLUMN]):
        msg_length = str(row[MESSAGE_LENGTH_COLUMN])
    
    # 요약
    summary_str = "요약 정보가 없습니다."
    if SUMMARY_COLUMN in row.index and pd.notnull(row[SUMMARY_COLUMN]):
        summary_str = str(row[SUMMARY_COLUMN]).strip()
    summary_str = html.escape(summary_str)
    
    # 정규화된 텍스트 (미리보기용)
    preview_text = ""
    if NORMALIZED_TEXT_COLUMN in row.index and pd.notnull(row[NORMALIZED_TEXT_COLUMN]):
        preview_text = str(row[NORMALIZED_TEXT_COLUMN])[:200]
        if len(str(row[NORMALIZED_TEXT_COLUMN])) > 200:
            preview_text += "..."
    preview_text = html.escape(preview_text)
    
    # 전체 메시지
    full_message = "메시지 없음"
    if MESSAGE_COLUMN in row.index and pd.notnull(row[MESSAGE_COLUMN]):
        full_message = str(row[MESSAGE_COLUMN])
    full_message = html.escape(full_message)
    
    html_output = f"""
    <details>
        <summary style="cursor: pointer; padding: 10px; background-color: #f0f2f6; border-radius: 5px; margin-bottom: 10px;">
            <span style="font-size: 18px;"><b>📅 {date_str}</b> | <b>📺 {channel_str}</b></span><br>
            <span style="font-size: 14px; color: #666;">
                {summary_str}
            </span><br>
            <span style="font-size: 13px;">
                🏷️ {labels_str} | {sentiment_emoji} {sentiment_str} | 📏 {msg_length}자
            </span>
        </summary>
        <div style='margin-top: 15px; padding: 15px; background-color: #ffffff; border-left: 3px solid #4CAF50; border-radius: 5px;'>
            <h4>📌 요약</h4>
            <p style="white-space: pre-wrap; line-height: 1.6;">{summary_str}</p>
            
            <h4>🔑 키워드</h4>
            <p><span style="background-color: #e3f2fd; padding: 3px 8px; border-radius: 3px; margin-right: 5px;">{keywords_str}</span></p>
            
            <h4>💬 전체 메시지</h4>
            <div style="background-color: #f9f9f9; padding: 10px; border-radius: 5px; max-height: 400px; overflow-y: auto;">
                <pre style="white-space: pre-wrap; font-family: inherit; font-size: 13px; margin: 0;">{full_message}</pre>
            </div>
            
            <hr style="margin: 15px 0;">
            <p style="font-size: 12px; color: #888;">
                <b>채널:</b> {channel_str} | <b>감성:</b> {sentiment_emoji} {sentiment_str} | <b>길이:</b> {msg_length}자
            </p>
        </div>
    </details>
    """
    return html_output

# 페이지 데이터 출력
if not page_data.empty:
    for idx, row in page_data.iterrows():
        st.markdown(format_message(row), unsafe_allow_html=True)
        st.markdown("---")
else:
    st.info("표시할 메시지가 없습니다.")

# 사이드바에 사용 가이드 추가
st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📖 사용 가이드
1. **날짜 범위**: 원하는 기간의 메시지를 선택
2. **채널**: 특정 텔레그램 채널 선택
3. **라벨**: Company, Memory, Product, Tech 등
4. **감성**: 긍정적/부정적/중립적 필터링
5. **키워드**: 메시지 내용으로 검색
6. **페이지**: 한 페이지에 10개씩 표시

### 💡 팁
- 여러 필터를 조합하여 원하는 메시지만 선별
- 키워드 검색으로 특정 주제 집중 분석
- 라벨과 감성 분석으로 트렌드 파악
""")

# 하단에 데이터 다운로드 옵션
if not filtered_data.empty:
    st.sidebar.markdown("---")
    csv = filtered_data.to_csv(index=False, encoding='utf-8-sig')
    st.sidebar.download_button(
        label="📥 필터링된 데이터 다운로드 (CSV)",
        data=csv,
        file_name=f"filtered_messages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
