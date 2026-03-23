import streamlit as st
import pandas as pd
import json
import os
import hashlib
import plotly.express as px
import pyproj
import zipfile

# Auto-extract data bundle on cloud platform startup
if not os.path.exists("./data/0321/data_dump.json") and os.path.exists("./app_data.zip"):
    with zipfile.ZipFile("./app_data.zip", 'r') as zf:
        zf.extractall("./")

st.set_page_config(page_title="로컬 힙 타겟 맵핑 솔루션", page_icon="🎯", layout="wide")

st.title("🎯 2030 일본 여성 타겟 '로컬 힙(Local Hip)' 맵핑 대시보드")
st.markdown("거시적 데이터 진단을 넘어, 최적의 타겟 지역과 코스를 제안합니다.")

dump_path = "./data/0321/data_dump.json"
infra_path = "./data/0321/matjip/nationwide_summary.json"
expanded_pool_path = "./data/0321/matjip/expanded_pool.parquet"
tourist_csv_path = "./data/0321/matjip/fulldata_07_24_01_P_관광식당.csv"

transformer_5174 = pyproj.Transformer.from_crs("epsg:5174", "epsg:4326", always_xy=True)

def parse_coords(val_x, val_y):
    try:
        if pd.isna(val_x) or pd.isna(val_y): return None, None
        x, y = float(val_x), float(val_y)
        if 120 < x < 135 and 30 < y < 45: return y, x # already WGS84 lon/lat
        if 120 < y < 135 and 30 < x < 45: return x, y # flipped
        
        # Try converting epsg5174 -> wgs84
        lon, lat = transformer_5174.transform(x, y)
        if 120 < lon < 140 and 30 < lat < 45: return lat, lon
        
        # Try local epsg2097 just in case
        t2097 = pyproj.Transformer.from_crs("epsg:2097", "epsg:4326", always_xy=True)
        lon2, lat2 = t2097.transform(x, y)
        if 120 < lon2 < 140 and 30 < lat2 < 45: return lat2, lon2
    except: pass
    return None, None

@st.cache_data
def load_data():
    if not os.path.exists(dump_path) or not os.path.exists(infra_path): return None, None
    with open(dump_path, 'r', encoding='utf-8') as f: dump = json.load(f)
    with open(infra_path, 'r', encoding='utf-8') as f: infra = json.load(f)
    return dump, infra

dump, infra = load_data()
if not dump: st.stop()

# Build Intro Matrix
df_cafe = pd.DataFrame(list(infra['cafe_bakery'].items()), columns=['시도명', '카페수'])
df_tour = pd.DataFrame(list(infra['tourist_restaurant'].items()), columns=['시도명', '관광식당수'])
df_gen = pd.DataFrame(list(infra['general_restaurant'].items()), columns=['시도명', '일반식당수'])
df_infra = df_cafe.merge(df_tour, on='시도명', how='outer').merge(df_gen, on='시도명', how='outer').fillna(0)
df_infra['총_식음료인프라'] = df_infra['카페수'] + df_infra['관광식당수'] + df_infra['일반식당수']

sido_map = {"서울특별시": "서울", "부산광역시": "부산", "대구광역시": "대구", "인천광역시": "인천", 
            "광주광역시": "광주", "대전광역시": "대전", "울산광역시": "울산", "세종특별자치시": "세종",
            "경기도": "경기", "강원특별자치도": "강원", "충청북도": "충북", "충청남도": "충남", 
            "전북특별자치도": "전북", "전라남도": "전남", "경상북도": "경북", "경상남도": "경남", "제주특별자치도": "제주"}

visit_data = []
for row in dump["P2-8 일본인 지역별 방문비율월별"]:
    sido_full = row.get("일본인 광역별 방문비율 월별 추이 (2024.01~2025.12)")
    avg_ratio = row.get("Unnamed: 25")
    if sido_full in sido_map and isinstance(avg_ratio, (float, int)):
        visit_data.append({"시도명": sido_map[sido_full], "평균방문비율(%)": avg_ratio})

df_visit = pd.DataFrame(visit_data)
df_infra['시도_abbr'] = df_infra['시도명'].map(lambda x: sido_map.get(x, x))
df_matrix = pd.merge(df_infra, df_visit, left_on='시도_abbr', right_on='시도명', suffixes=('_full', ''))

franchise_keywords = ['스타벅스', '이디야', '투썸', '메가커피', '컴포즈', '빽다방', '파스쿠찌', '할리스', '폴바셋', '더벤티', '엔제리너스', '탐앤탐스', '파리바게뜨', '뚜레쥬르', '배스킨라빈스', '던킨', '맥도날드', '롯데리아', '버거킹', '맘스터치', 'KFC', '써브웨이', '아웃백', '빕스', '애슐리', '홍콩반점', '롤링파스타', '김밥천국', '고봉민', '신전', '죠스', '엽기떡볶이', '본죽', '비비큐', '교촌', 'BHC', '굽네', '60계', '노랑통닭', '블루보틀', '설빙', '공차', '매머드', '더리터', '요거프레소', '이삭토스트', '백종원', '파파존스', '도미노', '피자헛', '하남돼지', '명륜진사', '새마을식당', '지코바', '이차돌', '팔각도', '한솥', '역전할머니', '투다리', '크라운호프', '청년다방', '명랑핫', '두찜', '김가네', '얌샘', '얌스']

bad_keywords = ['당구', 'PC', '피시', '노래', '단란', '주점', '유흥', '골프', '스크린', '뽑기', '복권', '다트', '보드게임', '모텔', '출장', '장례', '구내식당', '요양', '학원', '어린이집', '독서실', '부동산', '휴게소', '장어', '추어탕', '보신탕', '마사지', '피부', '미용', '공인중개']

def is_franchise(n):
    n_str = str(n).replace(" ","").upper()
    for b in franchise_keywords + bad_keywords:
        if b.upper() in n_str: return True
    if n_str.endswith('점') and not n_str.endswith('본점') and not n_str.endswith('전문점') and not n_str.endswith('음식점'): return True
    return False

def guess_food_type(name, raw_cat=None):
    c = str(raw_cat).replace(' ', '') if pd.notna(raw_cat) else ""
    n = str(name).replace(' ', '') if pd.notna(name) else ""
    if any(k in n for k in ['커피', '카페', '제과', '빵', '베이커리', '다방', '라운지', '로스터', '로스터리', '에스프레소', '마카롱', '디저트', '케이크', '구움과자', '샌드위치', '찻집', '티룸', '티하우스', '젤라또', '크로플', '도넛', '도낫']): return "☕ 카페/디저트"
    if any(k in c for k in ['다방', '카페', '커피', '제과', '전통차']): return "☕ 카페/디저트"
    if any(k in c for k in ['중국식', '중식', '중화', '마라']): return "🍜 중식/마라"
    if any(k in c for k in ['일식', '생선회', '횟집', '참치']): return "🐟 해산물/일식"
    if any(k in n for k in ['피자', '파스타', '레스토랑', '양식', '비스트로', '브런치', '이탈리안', '프렌치', '다이닝', '스테이크']): return "🍝 양식/패스트푸드"
    if any(k in n for k in ['고기', '갈비', '삼겹', '한우', '막창', '식육', '곱창', '대창', '화로', '바베큐', '우대']): return "🥩 K-바베큐 (고기)"
    if any(k in n for k in ['회', '수산', '바다', '초밥', '스시', '해물', '조개', '게장', '대게', '어시장']): return "🐟 해산물/일식"
    if any(k in n for k in ['국수', '밀면', '냉면', '칼국수', '모밀', '면옥', '우동', '소바', '라멘', '짬뽕', '막국수']): return "🥢 면요리"
    if '가든' in n or '정육' in n: return "🥩 K-바베큐 (고기)"
    return "🍚 모던 로컬 한식"

def generate_reason(name, cat, city):
    c = str(cat)
    if '카페' in c: return f"프랜차이즈에선 느낄 수 없는 {city} 고유의 감성. '#韓国カフェ巡り'를 외치는 일본 2030의 니즈 완벽 충족."
    elif '해산물' in c: return f"{city}의 지리적 매력을 직관적으로 보여주는 로컬 해산물 명소."
    elif '바베큐' in c: return f"K-바베큐의 진수. '나만 아는 현지인 로컬 핫플'로 인스타 릴스 스토리에 최적화."
    elif '양식' in c: return f"로컬 식재료를 재해석한 다이닝. 트렌디한 분위기로 인증샷에 열광하는 2030 타겟팅 적중."
    else: return f"화려한 서울을 벗어나 '진짜 한국인의 삶'에 밀착하고 싶어 하는 N차 방문객에게 최고의 미식 경험 제공."

def generate_proxy_scores(name):
    # Base randomized proxy score
    seed = int(hashlib.md5(str(name).strip().encode('utf-8')).hexdigest(), 16)
    base_score = 50 + (seed % 30)
    
    # Target Preference Weighting (+40 points for Japanese 2030s Women Trends)
    target_keywords = ['베이커리', '다이닝', '라운지', '디저트', '제과', '로스터', '로스터리', '이탈리안', '브런치', '오션뷰', '당', '관', '옥', '가', '스튜디오', '아뜰리에', '비스트로', '스테이크', '오마카세', '카츠', '솥밥', '정식']
    penalty_keywords = ['해장국', '기사식당', '호프', '노가리', '아구찜', '동태', '생태', '홍어']
    
    n_str = str(name).replace(" ", "")
    bonus = 40 if any(kw in n_str for kw in target_keywords) else 0
    penalty = -50 if any(kw in n_str for kw in penalty_keywords) else 0
    
    return min(99, max(1, base_score + bonus + penalty))

@st.cache_data
def get_recommendations(target_sido):
    dfs = []
    
    def process_df(df, is_parquet=False):
        op_col = '상세영업상태명' if '상세영업상태명' in df.columns else ('영업상태명' if '영업상태명' in df.columns else None)
        if op_col: df = df[df[op_col].astype(str).str.contains('영업|정상', na=False)]
        
        addr_col = next((c for c in ['주소', '도로명전체주소', '도로명주소', '소재지전체주소', '상세주소'] if c in df.columns), None)
        if not addr_col: return None
        filt = df[addr_col].astype(str).str.contains(target_sido, na=False)
        if '지역_풀네임' in df.columns: filt = filt | df['지역_풀네임'].astype(str).str.contains(target_sido, na=False)
        df_f = df[filt].copy()
        
        if df_f.empty: return None
        
        df_f['사업장명'] = df_f['사업장명'].astype(str)
        df_f['카테고리'] = df_f.apply(lambda row: guess_food_type(row['사업장명'], row.get('업태구분명', None)), axis=1)
        df_f['주소'] = df_f[addr_col]
        
        # Coords
        hx = next((c for c in df_f.columns if 'x' in c.lower() and ('좌표' in c or 'epsg' in c.lower())), None)
        hy = next((c for c in df_f.columns if 'y' in c.lower() and ('좌표' in c or 'epsg' in c.lower())), None)
        if not hx: hx = next((c for c in df_f.columns if '경도' in c), None)
        if not hy: hy = next((c for c in df_f.columns if '위도' in c), None)
        df_f['lat'] = None
        df_f['lon'] = None
        if hx and hy:
            coords = df_f.apply(lambda r: parse_coords(r[hx], r[hy]), axis=1)
            df_f['lat'] = [c[0] for c in coords]
            df_f['lon'] = [c[1] for c in coords]
            
        return df_f[['사업장명', '카테고리', '주소', 'lat', 'lon']]

    if os.path.exists(tourist_csv_path):
        try: df_t = pd.read_csv(tourist_csv_path, encoding='cp949', low_memory=False)
        except: df_t = pd.read_csv(tourist_csv_path, encoding='utf-8', low_memory=False)
        res = process_df(df_t)
        if res is not None: dfs.append(res)

    if os.path.exists(expanded_pool_path):
        df_e = pd.read_parquet(expanded_pool_path)
        res = process_df(df_e, True)
        if res is not None: dfs.append(res)

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        combined = combined[~combined['사업장명'].str.contains('PC|피시|게임', case=False, na=False)]
        combined = combined[~combined['사업장명'].apply(is_franchise)].drop_duplicates(subset=['사업장명']).reset_index(drop=True)
        if combined.empty: return combined
        combined['AI_Score'] = combined['사업장명'].apply(generate_proxy_scores)
        combined['선정이유'] = combined.apply(lambda x: generate_reason(x['사업장명'], x['카테고리'], target_sido), axis=1)
        return combined.sort_values('AI_Score', ascending=False)
    return pd.DataFrame()

@st.cache_data
def load_all_kdrama():
    import glob
    dir_k = "./data/0321/5. 관광문화 트렌드 빅데이터/1.K-DRAMA(24.11-26.01)/K-DRAMA(24.11-26.01)"
    files = glob.glob(os.path.join(dir_k, "*.csv"))
    dfs = []
    for f in files:
        try: dfs.append(pd.read_csv(f, encoding='utf-8', usecols=['TRRSRT_NM', 'PLACE_TY', 'ADDR', 'CTPRVN_NM', 'SIGNGU_NM', 'LC_LA', 'LC_LO']))
        except: pass
    if dfs:
        df = pd.concat(dfs, ignore_index=True).dropna(subset=['TRRSRT_NM'])
        agg_df = df.groupby(['TRRSRT_NM', 'PLACE_TY', 'ADDR', 'CTPRVN_NM', 'SIGNGU_NM', 'LC_LA', 'LC_LO']).size().reset_index(name='SNS언급량')
        return agg_df.sort_values(by='SNS언급량', ascending=False)
    return pd.DataFrame()

# Shared Target Logic
blue_ocean_cities = ['부산 (부산)', '포항 (경북)', '강릉 (강원)', '제주 (제주)', '경주 (경북)', '속초 (강원)', '여수 (전남)', '전주 (전북)', '순천 (전남)', '통영 (경남)', '거제 (경남)', '목포 (전남)', '안동 (경북)']
target_cities = st.multiselect(
    "📌 맵핑 지역 복수 선택 (블루오션 유망도 랭킹 순 정렬 기준):", 
    blue_ocean_cities, 
    default=['부산 (부산)']
)
search_cities = [tc.split(' ')[0] for tc in target_cities]
st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["📊 1. 거시적 수요 진단 (KPI)", "📍 2. 데이터 교차 매트릭스", "🎯 3. 타겟 핫플 코스 제안", "🗺️ 4. 로컬 통합 시각화 맵"])

with tab1:
    st.subheader("1. 일본인 2030 여성 서울 편중 현상 및 관심사 진단 (The WHY)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("일본인 입국자 중 여성 비율", "67.6%", "압도적 여초 현상")
    c2.metric("2025.02 일본 방문객 증감률", "-12.28%", "매력도 하락 (피로도 증가)", delta_color="inverse")
    c3.metric("외국인 간편결제 서울 집중도", "88.1%", "지방 인프라 부족")
    c4.metric("일본 SNS 내 '#서울' 1위 연속월", "24개월 연속", "강력한 정보 쏠림 향상")
    
    st.markdown("---")
    st.markdown("#### 🔍 일본인 한국 관광/문화 관심 키워드 TOP 순위 (24~25년 누적)")
    st.markdown("서울 외 지역으로 유인하기 위해서는 타겟이 가장 열광하는 **'K-콘텐츠(드라마/K팝/영화)'**를 로컬 관광의 핵심 무기로 삼아야 합니다.")
    
    kw_path = "./data/0321/keyword_2024_2025.csv"
    if os.path.exists(kw_path):
        df_kw = pd.read_csv(kw_path)
        # Filter out generic or controversy words to focus on culture/travel
        ignore_words = ['사건', '논란', '경찰', '사망', '음주운전', '조사', '혐의', '죽음', '범죄', '시즌', '작품', '감독', '배우', '가족', '학생', '여성']
        df_kw_filtered = df_kw[~df_kw['KWRD_NM'].isin(ignore_words)]
        top_kw = df_kw_filtered.groupby('KWRD_NM')['TOT_CAS_CO'].sum().sort_values(ascending=False).head(12).reset_index()
        
        fig_kw = px.bar(top_kw, x='TOT_CAS_CO', y='KWRD_NM', orientation='h', 
                        title="일본 SNS 한국 관련 주요 검색 키워드 (K콘텐츠 압도적 우위)",
                        labels={'TOT_CAS_CO': '총 검색/언급량', 'KWRD_NM': '키워드'},
                        color='TOT_CAS_CO', color_continuous_scale='Sunset')
        fig_kw.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
        st.plotly_chart(fig_kw, use_container_width=True)
        
    st.info("💡 **인사이트:** 타겟의 핵심 관심사는 압도적으로 **'K팝, 영화, 드라마, 팬덤'**에 쏠려 있습니다. 이를 지방의 '인스타 감성 카페', '로컬 맛집'과 결합하여 완벽한 하루 동선을 제공해야 합니다.")

with tab2:
    st.subheader("2. 어느 지역으로 분산시켜야 하는가? (The WHERE)")
    df_chart = df_matrix[~df_matrix['시도_abbr'].isin(['서울', '경기'])].copy()
    fig = px.scatter(df_chart, x="평균방문비율(%)", y="총_식음료인프라", text="시도_abbr", size="카페수", color="시도_abbr",
                     title="로컬 힙 잠재력 산점도 (Bubble Size: 카페 인프라 강도)", height=500)
    fig.update_traces(textposition='top center')
    st.plotly_chart(fig, use_container_width=True)
    st.success("🏆 '부산', '경남(포항)', '강원(강릉)' 등은 카페 인프라가 훌륭하여 블루오션 가치가 높습니다.")

with st.spinner("데이터 분석 및 다중 매칭 중..."):
    df_foods_list, df_cafes_list, df_k_list = [], [], []
    df_k_all = load_all_kdrama()
    
    for search_city, target_city in zip(search_cities, target_cities):
        df_target = get_recommendations(search_city)
        if df_target is not None and not df_target.empty:
            df_all_cafes = df_target[df_target['카테고리'].str.contains('카페', na=False)]
            df_all_foods = df_target[~df_target['카테고리'].str.contains('카페', na=False)]
            
            diverse_foods = df_all_foods.drop_duplicates(subset=['카테고리'], keep='first')
            if len(diverse_foods) < 5: 
                df_f = pd.concat([diverse_foods, df_all_foods[~df_all_foods['사업장명'].isin(diverse_foods['사업장명'])]]).head(5)
            else: df_f = diverse_foods.head(5)
                
            df_c = df_all_cafes.head(5)
            
            df_f = df_f.copy()
            df_f['TargetCity'] = target_city
            df_c = df_c.copy()
            df_c['TargetCity'] = target_city
            
            df_foods_list.append(df_f)
            df_cafes_list.append(df_c)
            
        if not df_k_all.empty:
            df_k_target = df_k_all[df_k_all['ADDR'].astype(str).str.contains(search_city) | df_k_all['SIGNGU_NM'].astype(str).str.contains(search_city)].copy()
            df_k_top5 = df_k_target.drop_duplicates(subset=['TRRSRT_NM']).head(5).copy()
            if not df_k_top5.empty:
                df_k_top5['TargetCity'] = target_city
                df_k_list.append(df_k_top5)

    df_foods = pd.concat(df_foods_list, ignore_index=True) if df_foods_list else pd.DataFrame()
    df_cafes = pd.concat(df_cafes_list, ignore_index=True) if df_cafes_list else pd.DataFrame()
    df_k_top5 = pd.concat(df_k_list, ignore_index=True) if df_k_list else pd.DataFrame()

with tab3:
    st.subheader("3. 로컬 힙(Local Hip) 다중 핫플 코스 제안 (텍스트 전용)")
    
    cities_str = ", ".join(target_cities) if target_cities else "선택된 지역 없음"
    st.markdown(f"### 🎬 {cities_str} K-Drama 성지 / 관광 명소 통합 TOP 5")
    if not df_k_top5.empty:
        for i, row in df_k_top5.iterrows():
            with st.expander(f"📸 [{row['TargetCity']}] 핫플 명소: {row['TRRSRT_NM']} ({row['PLACE_TY']})"):
                st.markdown(f"**📌 주소:** {row['ADDR']}")
                st.markdown(f"**💬 K-Drama 버즈량:** {row['SNS언급량']}회")
    else: st.info("매칭된 데이터가 없습니다.")

    st.markdown(f"### 🍽️ {cities_str} 로컬 맛집 통합 풀 리스트 (카테고리 다변화)")
    if not df_foods.empty:
        for i, row in df_foods.iterrows():
            with st.expander(f"🏅 [{row['TargetCity']}] {row['사업장명']} ({row['카테고리']})"):
                st.markdown(f"**📌 주소:** {row['주소']}")
                st.markdown(f"**💬 선정 이유:** {row['선정이유']}")
    else: st.info("매칭된 데이터가 없습니다.")

    st.markdown(f"### ☕ {cities_str} 로컬 인스타 감성 카페 통합 리스트")
    if not df_cafes.empty:
        for i, row in df_cafes.iterrows():
            with st.expander(f"🏅 [{row['TargetCity']}] {row['사업장명']}"):
                st.markdown(f"**📌 주소:** {row['주소']}")
                st.markdown(f"**💬 선정 이유:** {row['선정이유']}")
    else: st.info("매칭된 데이터가 없습니다.")

with tab4:
    cities_str = ", ".join(target_cities) if target_cities else "지역"
    st.subheader(f"🗺️ 4. {cities_str} 로컬 다중 통합 시각화 맵")
    st.markdown("선택한 모든 지역의 관광 명소(K-Drama), 추천 맛집, 추천 카페를 한눈에 볼 수 있는 동선 기획용 맵입니다. \n\n* **빨간색(Red):** 식당 맛집  \n* **파란색(Blue):** 감성 카페  \n* **초록색(Green):** K-Drama 관광명소")
    
    map_list = []
    
    if not df_foods.empty:
        for idx, row in df_foods.dropna(subset=['lat', 'lon']).iterrows():
            map_list.append({'이름': row['사업장명'], 'latitude': row['lat'], 'longitude': row['lon'], 'color': '#FF0000', 'type': '맛집', 'TargetCity': row['TargetCity']})
            
    if not df_cafes.empty:
        for idx, row in df_cafes.dropna(subset=['lat', 'lon']).iterrows():
            map_list.append({'이름': row['사업장명'], 'latitude': row['lat'], 'longitude': row['lon'], 'color': '#0000FF', 'type': '카페', 'TargetCity': row['TargetCity']})
            
    if not df_k_top5.empty:
        for idx, row in df_k_top5.dropna(subset=['LC_LA', 'LC_LO']).iterrows():
            map_list.append({'이름': row['TRRSRT_NM'], 'latitude': row['LC_LA'], 'longitude': row['LC_LO'], 'color': '#00FF00', 'type': 'K-Drama 명소', 'TargetCity': row['TargetCity']})
            
    df_map = pd.DataFrame(map_list)
    if not df_map.empty and 'TargetCity' in df_map.columns:
        valid_map_frames = []
        # Handle outliers cleanly grouping by city so separate clusters can exist without breaking each other
        for city, group in df_map.groupby('TargetCity'):
            med_lat = group['latitude'].median()
            med_lon = group['longitude'].median()
            valid = group[
                (group['latitude'] >= med_lat - 0.25) & (group['latitude'] <= med_lat + 0.25) &
                (group['longitude'] >= med_lon - 0.25) & (group['longitude'] <= med_lon + 0.25)
            ]
            valid_map_frames.append(valid)
            
        final_valid_map = pd.concat(valid_map_frames, ignore_index=True) if valid_map_frames else pd.DataFrame()
        
        if not final_valid_map.empty:
            st.map(final_valid_map, latitude='latitude', longitude='longitude', color='color', zoom=None, use_container_width=True)
        else:
            st.warning("선택하신 각 지역의 유효한 반경 내 좌표 데이터를 찾지 못했습니다.")
    else:
        st.warning("선택하신 지역의 위치 데이터(좌표)를 추출할 수 없어 지도를 생성하지 못했습니다.")
