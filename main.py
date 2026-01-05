import streamlit as st
import pandas as pd
import time
import os
import duckdb
import glob
import gc
from datetime import datetime, timedelta, timezone
from supabase import create_client
from st_aggrid import AgGrid, GridOptionsBuilder

# ==================================================
# 定数定義
# ==================================================
pd.set_option('future.no_silent_downcasting', True)

try:
    auth_config = st.secrets["auth"]
    USER_LIST = auth_config["users"]
    ADMIN_TEAM_USERS = auth_config["admin_users"]
except KeyError:
    st.error("認証設定(secrets.toml)が読み込めません。[auth]セクションに users と admin_users を設定してください。")
    st.stop()

GENERAL_TEAM_USERS = [u for u in USER_LIST if u not in ADMIN_TEAM_USERS]


GENRE_MAP = {
    "0": "未選択〔未選択〕",
    "101": "異世界〔恋愛〕",
    "102": "現実世界〔恋愛〕",
    "201": "ハイファンタジー〔ファンタジー〕",
    "202": "ローファンタジー〔ファンタジー〕",
    "301": "純文学〔文芸〕",
    "302": "ヒューマンドラマ〔文芸〕",
    "303": "歴史〔文芸〕",
    "304": "推理〔文芸〕",
    "305": "ホラー〔文芸〕",
    "306": "アクション〔文芸〕",
    "307": "コメディー〔文芸〕",
    "401": "VRゲーム〔SF〕",
    "402": "宇宙〔SF〕",
    "403": "空想科学〔SF〕",
    "404": "パニック〔SF〕",
    "9901": "童話〔その他〕",
    "9902": "詩〔その他〕",
    "9903": "エッセイ〔その他〕",
    "9904": "リプレイ〔その他〕",
    "9999": "その他〔その他〕",
    "9801": "ノンジャンル〔ノンジャンル〕"
}

# ==================================================
# Page config
# ==================================================
st.set_page_config(
    page_title="なろう小説 ダッシュボード",
    layout="wide"
)

# ==================================================
# セキュリティ
# ==================================================
st.sidebar.header("ログイン")

qp = st.query_params
default_user = qp.get("username", qp.get("user", ""))
default_pass = qp.get("password", qp.get("pass", ""))

user_name = st.sidebar.text_input("ユーザー名", value=default_user)
password = st.sidebar.text_input("パスワード", type="password", value=default_pass)

if user_name not in USER_LIST or password != st.secrets["auth"]["password"]:
    st.warning("登録されたユーザー名と正しいパスワードを入力してください")
    st.stop()

if user_name in ADMIN_TEAM_USERS:
    st.sidebar.success("原作管理チーム")
elif user_name in USER_LIST:
    st.sidebar.info("一般編集")

# ==================================================
# DB 接続
# ==================================================
@st.cache_resource
def init_supabase():
    return create_client(
        st.secrets["supabase"]["url"],
        st.secrets["supabase"]["key"]
    )

supabase = init_supabase()

@st.cache_resource
def get_db_connection():
    conn = duckdb.connect(database=':memory:')
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parquet_pattern = os.path.join(base_dir, "narou_novels_part*.parquet")
    
    import glob
    parquet_files = glob.glob(parquet_pattern)
    if not parquet_files:
        st.error(f"データファイルが見つかりません: {parquet_pattern}")
        return None

    safe_files = [f.replace(os.sep, '/') for f in parquet_files]
    escaped_files = [f.replace("'", "''") for f in safe_files]
    file_list_str = ', '.join([f"'{f}'" for f in escaped_files])
    
    query = f"""
        CREATE OR REPLACE VIEW master_novels AS 
        SELECT 
            ncode, title, writer, genre, keyword,
            TRY_CAST(general_firstup AS TIMESTAMP) as general_firstup,
            TRY_CAST(general_lastup AS TIMESTAMP) as general_lastup,
            TRY_CAST(REPLACE(CAST(general_all_no AS VARCHAR), ',', '') AS BIGINT) AS general_all_no,
            length,
            TRY_CAST(REPLACE(CAST(global_point AS VARCHAR), ',', '') AS BIGINT) AS global_point,
            TRY_CAST(REPLACE(CAST(daily_point AS VARCHAR), ',', '') AS BIGINT) AS daily_point,
            TRY_CAST(REPLACE(CAST(weekly_point AS VARCHAR), ',', '') AS BIGINT) AS weekly_point,
            TRY_CAST(REPLACE(CAST(monthly_point AS VARCHAR), ',', '') AS BIGINT) AS monthly_point,
            TRY_CAST(REPLACE(CAST(fav_novel_cnt AS VARCHAR), ',', '') AS BIGINT) AS fav_novel_cnt,
            TRY_CAST(REPLACE(CAST(all_point AS VARCHAR), ',', '') AS BIGINT) AS all_point,
            TRY_CAST(novelupdated_at AS TIMESTAMP) as novelupdated_at,
            TRY_CAST(REPLACE(CAST(weekly_unique AS VARCHAR), ',', '') AS BIGINT) AS weekly_unique,
            story
        FROM read_parquet([{file_list_str}])
    """
    try:
        conn.execute(query)
        conn.execute("CREATE TABLE IF NOT EXISTS novel_status (ncode VARCHAR, is_ng BOOLEAN, is_admin_evaluated BOOLEAN, is_admin_rejected BOOLEAN, is_general_evaluated BOOLEAN, is_general_rejected BOOLEAN)")
        conn.execute("CREATE TABLE IF NOT EXISTS user_ratings_raw (ncode VARCHAR, user_name VARCHAR, rating VARCHAR, comment VARCHAR, role VARCHAR, updated_at VARCHAR)")
    except Exception as e:
        st.error(f"DuckDB初期化エラー: {e}")
        return None
        
    return conn

@st.cache_data(ttl=300)
def sync_ratings_to_db(_conn):
    try:
        df_ratings = load_all_ratings_table()
        
        _conn.execute("DROP TABLE IF EXISTS user_ratings_raw")
        _conn.execute("DROP TABLE IF EXISTS novel_status")

        if df_ratings.empty:
            _conn.execute("CREATE TABLE user_ratings_raw (ncode VARCHAR, user_name VARCHAR, rating VARCHAR, comment VARCHAR, role VARCHAR, updated_at VARCHAR)")
            _conn.execute("CREATE TABLE novel_status (ncode VARCHAR, is_ng BOOLEAN, is_admin_evaluated BOOLEAN, is_admin_rejected BOOLEAN, is_general_evaluated BOOLEAN, is_general_rejected BOOLEAN)")
        else:
            _conn.register('temp_ratings_source', df_ratings)
            _conn.execute("CREATE TABLE user_ratings_raw AS SELECT * FROM temp_ratings_source")
            _conn.unregister('temp_ratings_source')
            
            df_status = calculate_novel_status(df_ratings)
            if not df_status.empty:
                _conn.register('temp_status_source', df_status)
                _conn.execute("CREATE TABLE novel_status AS SELECT * FROM temp_status_source")
                _conn.unregister('temp_status_source')
            else:
                 _conn.execute("CREATE TABLE novel_status (ncode VARCHAR, is_ng BOOLEAN, is_admin_evaluated BOOLEAN, is_admin_rejected BOOLEAN, is_general_evaluated BOOLEAN, is_general_rejected BOOLEAN)")
                 
        return datetime.now().strftime("%H:%M:%S")
    except Exception as e:
        return None

def load_novel_story(ncode):
    conn = get_db_connection()
    if not conn: return "情報なし"
    try:
        res = conn.execute("SELECT story FROM master_novels WHERE ncode = ?", [ncode]).fetchone()
        return res[0] if res else "情報なし"
    except Exception as e:
        return f"あらすじ取得エラー: {str(e)}"

@st.cache_data(ttl=300)
def load_user_ratings(user_name):
    res = (
        supabase.table("user_ratings")
        .select("*")
        .eq("user_name", user_name)
        .execute()
    )
    return pd.DataFrame(res.data)

@st.cache_data(ttl=900)
def load_all_ratings_table():
    res = supabase.table("user_ratings").select("*").execute()
    return pd.DataFrame(res.data)

@st.cache_data(ttl=900)
def load_novel_ratings_all(ncode):
    try:
        res = (
            supabase.table("user_ratings")
            .select("*")
            .eq("ncode", ncode)
            .execute()
        )
        return pd.DataFrame(res.data)
    except Exception:
        return pd.DataFrame()

def get_jst_now():
    JST = timezone(timedelta(hours=9), 'JST')
    return datetime.now(JST).isoformat()


def save_rating(ncode, user_name, rating, comment, role):
    data = {
        "ncode": ncode,
        "user_name": user_name,
        "rating": rating,
        "comment": comment,
        "role": role,
        "updated_at": get_jst_now()
    }
    
    supabase.table("user_ratings").upsert(data, on_conflict="ncode,user_name").execute()
    
    if "local_rating_patches" not in st.session_state:
        st.session_state["local_rating_patches"] = {}
    
    st.session_state["local_rating_patches"][ncode] = {
        "rating": rating,
        "comment": comment,
        "role": role,
        "updated_at": data["updated_at"]
    }
    
    return True

def on_rating_button_click(ncode, user_name, target_rating, current_rating, role):
    comment = st.session_state.get(f"input_comment_area_{ncode}", "")
    
    new_rating = None if current_rating == target_rating else target_rating
    
    save_rating(ncode, user_name, new_rating, comment, role)

def save_comment_only(ncode, user_name, comment, role):
    current = load_user_ratings(user_name)
    current_rating = None
    if not current.empty:
        target = current[current["ncode"] == ncode]
        if not target.empty:
            current_rating = target.iloc[0]["rating"]
    
    data = {
        "ncode": ncode,
        "user_name": user_name,
        "rating": current_rating,
        "comment": comment,
        "role": role,
        "updated_at": get_jst_now()
    }
    
    supabase.table("user_ratings").upsert(data, on_conflict="ncode,user_name").execute()
    
    if "local_rating_patches" not in st.session_state:
        st.session_state["local_rating_patches"] = {}
        
    st.session_state["local_rating_patches"][ncode] = {
        "rating": current_rating,
        "comment": comment,
        "role": role,
        "updated_at": data["updated_at"]
    }

def determine_status(sub_df):
    flags = {
        "is_ng": False,
        "is_admin_evaluated": False,
        "is_admin_rejected": False,
        "is_general_evaluated": False,
        "is_general_rejected": False,
        "is_unclassified": False
    }

    valid_ratings_df = sub_df[sub_df["rating"].notna() & (sub_df["rating"] != "")]
    
    if valid_ratings_df.empty:
        flags["is_unclassified"] = True
        return flags

    ratings = set(valid_ratings_df["rating"].unique())
    
    if "NG" in ratings:
        flags["is_ng"] = True
        return flags
    
    admins_rated = valid_ratings_df[valid_ratings_df["user_name"].isin(ADMIN_TEAM_USERS)]
    if not admins_rated.empty:
        admin_ratings = set(admins_rated["rating"].unique())
        if any(r in ["〇", "○", "△"] for r in admin_ratings):
            flags["is_admin_evaluated"] = True
        else:
            flags["is_admin_rejected"] = True
        
    generals_rated = valid_ratings_df[valid_ratings_df["user_name"].isin(GENERAL_TEAM_USERS)]
    if not generals_rated.empty:
        gen_ratings = set(generals_rated["rating"].unique())
        if any(r in ["〇", "○", "△"] for r in gen_ratings):
            flags["is_general_evaluated"] = True
        else:
            flags["is_general_rejected"] = True

    if not any(flags.values()):
        flags["is_unclassified"] = True

    return flags


def calculate_novel_status(df_ratings):
    if df_ratings.empty:
        return pd.DataFrame()

    valid_mask = df_ratings["rating"].notna() & (df_ratings["rating"] != "")
    df_valid = df_ratings[valid_mask].copy()

    if df_valid.empty:
        return pd.DataFrame()

    df_valid["is_ng_flag"] = df_valid["rating"] == "NG"
    
    is_admin = df_valid["user_name"].isin(ADMIN_TEAM_USERS)
    is_general = df_valid["user_name"].isin(GENERAL_TEAM_USERS)
    
    is_positive = df_valid["rating"].isin(["〇", "○", "△"])
    
    df_valid["admin_pos"] = is_admin & is_positive
    df_valid["admin_neg"] = is_admin & ~is_positive & ~df_valid["is_ng_flag"]
    
    df_valid["gen_pos"] = is_general & is_positive
    df_valid["gen_neg"] = is_general & ~is_positive & ~df_valid["is_ng_flag"]
    
    grouped = df_valid.groupby("ncode")[["is_ng_flag", "admin_pos", "admin_neg", "gen_pos", "gen_neg"]].any()
    
    result = pd.DataFrame(index=grouped.index)
    result["ncode"] = grouped.index
    
    result["is_ng"] = grouped["is_ng_flag"]
    
    result["is_admin_evaluated"] = grouped["admin_pos"]
    result["is_admin_rejected"] = grouped["admin_neg"] & ~grouped["admin_pos"]
    
    result["is_general_evaluated"] = grouped["gen_pos"]
    result["is_general_rejected"] = grouped["gen_neg"] & ~grouped["gen_pos"]
    
    ng_mask = result["is_ng"]
    result.loc[ng_mask, ["is_admin_evaluated", "is_admin_rejected", "is_general_evaluated", "is_general_rejected"]] = False
    
    return result.reset_index(drop=True)


def annotate_novel_data(df_base, user_name):
    # DEPRECATED: This logic is now handled in execute_search_query
    return df_base


def apply_local_patches(df, user_name):
    if "local_rating_patches" not in st.session_state or not st.session_state["local_rating_patches"]:
        return df

    patches = st.session_state["local_rating_patches"]
    df_patched = df.copy()
    
    df_all_ratings = load_all_ratings_table()
    
    for ncode, patch in patches.items():
        if ncode in df_patched["ncode"].values:
            idx = df_patched[df_patched["ncode"] == ncode].index
            df_patched.loc[idx, "my_rating"] = patch["rating"]
            df_patched.loc[idx, "my_comment"] = patch["comment"]
        
        novel_ratings = df_all_ratings[df_all_ratings["ncode"] == ncode].copy()
        
        my_row_idx = novel_ratings[novel_ratings["user_name"] == user_name].index
        
        new_row = {
            "ncode": ncode,
            "user_name": user_name,
            "rating": patch["rating"],
            "comment": patch["comment"],
            "role": patch["role"],
            "updated_at": patch["updated_at"]
        }
        
        if not my_row_idx.empty:
            for k, v in new_row.items():
                novel_ratings.loc[my_row_idx, k] = v
        else:
            novel_ratings = pd.concat([novel_ratings, pd.DataFrame([new_row])], ignore_index=True)
            
        flags = determine_status(novel_ratings)
        
        if ncode in df_patched["ncode"].values:
            idx = df_patched[df_patched["ncode"] == ncode].index
            
            for flag_name, flag_val in flags.items():
                df_patched.loc[idx, flag_name] = flag_val
            
            def get_disp_status_single(row):
                if row["is_ng"]: return "NG"
                if row["is_admin_evaluated"]: return "Admin〇△"
                if row["is_admin_rejected"]: return "Admin×"
                if row["is_general_evaluated"]: return "Gen〇△"
                if row["is_general_rejected"]: return "Gen×"
                return "-"
            
            df_patched.loc[idx, "classification"] = df_patched.loc[idx].apply(get_disp_status_single, axis=1)

    return df_patched


# ==================================================
# UI
# ==================================================
st.markdown("""
<style>
    /* 全体のフォント設定 */
    .stApp {
        font-family: "Helvetica Neue", Arial, "Hiragino Kaku Gothic ProN", "Hiragino Sans", Meiryo, sans-serif;
    }
    
    /* タイトルスタイル */
    h1 {
        font-size: 2.2rem;
        font-weight: 700;
        color: #2c3e50;
        border-bottom: 2px solid #ecf0f1;
        padding-bottom: 10px;
        margin-bottom: 30px;
    }
    
    /* サブヘッダー */
    h3 {
        font-size: 1.4rem;
        font-weight: 600;
        color: #34495e;
        margin-top: 20px;
        margin-bottom: 15px;
    }
    
    /* カード風デザイン */
    .detail-card {
        background-color: #ffffff;
        padding: 24px;
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        margin-top: 20px;
    }
    
    /* ボタンのサイズ調整 */
    div.stButton > button {
        padding: 0.25rem 0.5rem;
        font-size: 0.85rem;
        min-height: auto;
    }
    
    /* ラベルのスタイル */
    .label {
        font-size: 0.85rem;
        color: #7f8c8d;
        margin-bottom: 4px;
    }
    .value {
        font-size: 1.1rem;
        color: #2c3e50;
        font-weight: 500;
        margin-bottom: 16px;
    }
    
    /* あらすじボックス */
    .story-box {
        background-color: #f8f9fa;
        padding: 16px;
        border-radius: 6px;
        line-height: 1.6;
        color: #4a5568;
        font-size: 0.95rem;
        border-left: 4px solid #3498db;
    }
    
    /* チェックボックスを黒に */
    div[data-testid="stSidebar"] div[data-testid="stCheckbox"]:has(input[id*="filter_netcon14"]) input[type="checkbox"]:checked {
        background-color: #000000 !important;
        border-color: #000000 !important;
    }
    div[data-testid="stSidebar"] div[data-testid="stCheckbox"]:has(input[id*="filter_netcon14"]) input[type="checkbox"] {
        border-color: #000000 !important;
    }
    div[data-testid="stSidebar"] div[data-testid="stCheckbox"]:has(input[id*="filter_netcon14"]) input[type="checkbox"]:checked::after {
        border-color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

st.title("なろう小説 ダッシュボード")

# DB接続
conn = get_db_connection()
if not conn:
    st.stop()
    
# データ同期
sync_status = sync_ratings_to_db(conn)
if sync_status:
    # st.caption(f"DB同期完了: {sync_status}")
    pass

st.session_state["data_loaded"] = True

# ==================================================
# 並び替え (Python側で実行)
# ==================================================
st.sidebar.header("並び替え")

sort_map = {
    "総合評価ポイント": "global_point",
    "日間ポイント": "daily_point",
    "作品の更新日時": "novelupdated_at",
    "Nコード": "ncode",
    "タイトル": "title",
    "著者": "writer",
    "ジャンル": "genre",
    "初回掲載日": "general_firstup",
    "最終掲載日": "general_lastup",
    "エピソード数": "general_all_no",
    "週間ユニークユーザー数": "weekly_unique",
}

default_sort_index = 0
if "日間ポイント" in sort_map:
    default_sort_index = list(sort_map.keys()).index("日間ポイント")
elif "総合評価ポイント" in sort_map:
    default_sort_index = list(sort_map.keys()).index("総合評価ポイント")

sort_col_label = st.sidebar.selectbox("並び替え項目", list(sort_map.keys()), index=default_sort_index, label_visibility="collapsed")
if st.session_state.get("data_loaded", False):
    sort_order = st.sidebar.radio("昇順・降順", ["降順", "昇順"], index=0, horizontal=True, label_visibility="collapsed")
else:
    if "sort_order" not in st.session_state:
        st.session_state["sort_order"] = "降順"
    sort_order = st.session_state["sort_order"]          

# ==================================================
# フィルタ
# ==================================================
st.sidebar.header("絞り込み")

genres = ["すべて"]
# DuckDBからジャンル一覧を取るのもありだが、固定リストを使用
sorted_genres = []
existing_genres = set(GENRE_MAP.values()) # 簡易的にMAP全値を使う
sorted_genres = sorted(list(existing_genres))
genres += sorted_genres

genre = st.sidebar.selectbox("ジャンル", genres)

col_check, col_label = st.sidebar.columns([0.15, 0.85])
with col_check:
    filter_netcon14 = st.checkbox("第14回ネットコン応募作品", value=True, key="filter_netcon14", label_visibility="collapsed")
with col_label:
    st.markdown('<p style="font-size: 0.8em; color: #666; margin-top: 0.5rem; margin-bottom: 0; text-align: left;">第14回ネット小説大賞応募作品を表示</p>', unsafe_allow_html=True)

search_keyword = st.sidebar.text_input("キーワード検索")
exclude_keyword = st.sidebar.text_input("検索除外ワード")

st.sidebar.markdown("---")
st.sidebar.caption("ポイントフィルタ")
min_global = st.sidebar.number_input("総合ポイント 以上", min_value=0, value=0, step=1000)
max_global = st.sidebar.number_input("総合ポイント 未満", min_value=0, value=0, step=1000)

st.sidebar.markdown("---")
st.sidebar.caption("日付フィルタ")
st.sidebar.text("初回掲載日")
col1, col2, col3 = st.sidebar.columns([2.5, 0.3, 2.5])
with col1:
    firstup_from = st.date_input("開始", value=datetime(2024, 2, 1).date(), key="firstup_from", label_visibility="collapsed")
with col2:
    st.markdown("<div style='text-align: center; padding-top: 0.5rem;'>～</div>", unsafe_allow_html=True)
with col3:
    firstup_to = st.date_input("終了", value=None, key="firstup_to", label_visibility="collapsed")
st.sidebar.text("最終掲載日")
col4, col5, col6 = st.sidebar.columns([2.5, 0.3, 2.5])
with col4:
    lastup_from = st.date_input("開始", value=None, key="lastup_from", label_visibility="collapsed")
with col5:
    st.markdown("<div style='text-align: center; padding-top: 0.5rem;'>～</div>", unsafe_allow_html=True)
with col6:
    lastup_to = st.date_input("終了", value=None, key="lastup_to", label_visibility="collapsed")
st.sidebar.caption("※初回掲載日が2024年2月1日以降の作品のみ対応")

# ==================================================
# データエクスポート
# ==================================================
st.sidebar.markdown("---")
with st.sidebar.expander("ヘルプ"):
    st.markdown("""
    <div style="font-size: 0.85rem; color: #555;">
    <b>初回掲載日</b><br>
    1エピソード目の投稿日<br><br>
    <b>最終掲載日</b><br>
    最新エピソードの投稿日<br><br>
    <b>総合評価pt</b><br>
    ＝(ブックマーク数*2)+評価ポイント<br><br>
    <b>日間ポイント</b><br>
    ランキング集計時点から過去24時間以内で新たに登録されたブックマークや評価が対象。毎日3回程度更新。<br><br>
    <b>週間UU数</b><br>
    前週の日曜日から土曜日分のユニークの合計。毎週火曜日早朝に更新。<br><br>
    <b>データ更新</b><br>
    毎朝6:30～7:00頃。<br><br>
    <hr>
    <b>評価優先順位</b><br>
    原作管理チーム、一般編集問わず、評価の中にNGがある場合はNGに振り分け。<br>
    <br>
    原作管理チームの中で○と△と×で評価が混在する場合、よりポジティブな評価を優先して振り分けされる。<br>
    ○＞△＞×<br>
    <br>
    一般編集の中で○と△と×で評価が混在する場合、よりポジティブな評価を優先して振り分けされる。<br>
    ○＞△＞×
    </div>
    """, unsafe_allow_html=True)

# エクスポート用：評価済み（ステータスあり）の作品だけを抽出して処理
if st.sidebar.button("評価済みリストをCSV出力作成"):
    try:
        with st.spinner("CSV作成中..."):
            # DuckDBから評価済みデータを結合して取得
            export_query = """
                SELECT 
                    t1.ncode as "Nコード",
                    t1.title as "タイトル",
                    t1.writer as "著者名",
                    t1.genre as "ジャンル",
                    strftime(t1.general_firstup, '%Y-%m-%d') as "初回掲載日",
                    strftime(t1.general_lastup, '%Y-%m-%d') as "最終掲載日",
                    t1.general_all_no as "話数",
                    t1.length as "文字数",
                    t1.global_point as "総合評価ポイント",
                    GROUP_CONCAT(
                        CASE WHEN t3.rating IS NOT NULL AND t3.rating != '' 
                             THEN t3.user_name || '：' || t3.rating 
                             ELSE NULL END, 
                        '　'
                    ) as "評価",
                    GROUP_CONCAT(
                        CASE WHEN t3.comment IS NOT NULL AND t3.comment != '' 
                             THEN t3.user_name || '：' || t3.comment 
                             ELSE NULL END, 
                        '　'
                    ) as "コメント"
                FROM master_novels t1
                JOIN user_ratings_raw t3 ON t1.ncode = t3.ncode
                WHERE t3.rating IS NOT NULL AND t3.rating != ''
                GROUP BY 
                    t1.ncode, t1.title, t1.writer, t1.genre, 
                    t1.general_firstup, t1.general_lastup, 
                    t1.general_all_no, t1.length, t1.global_point
                ORDER BY t1.ncode
            """
            df_export = conn.execute(export_query).df()
            
            if not df_export.empty:
                # ジャンルID変換
                df_export["ジャンル"] = df_export["ジャンル"].astype(str).map(GENRE_MAP).fillna(df_export["ジャンル"])
                
                csv_str = df_export.to_csv(index=False)
                csv_bytes = csv_str.encode('utf-8-sig')
                
                # session_stateに保存して再描画時も保持
                st.session_state["export_csv"] = csv_bytes
                st.session_state["export_time"] = datetime.now().strftime('%Y%m%d_%H%M%S')
            else:
                st.sidebar.warning("評価済みの作品はありません")
                if "export_csv" in st.session_state:
                    del st.session_state["export_csv"]

    except Exception as e:
        st.sidebar.error(f"エクスポートエラー: {e}")

if "export_csv" in st.session_state:
    st.sidebar.download_button(
        label="ダウンロード",
        data=st.session_state["export_csv"],
        file_name=f"reviewed_novels_{st.session_state['export_time']}.csv",
        mime="text/csv"
    )

# ==================================================
# リスト表示関数
# ==================================================
def render_novel_list(df_in, total_count, key_suffix, page, page_size):
    # df_in はすでにページングされた結果 (最大300件)
    
    # 日付フォーマット調整
    for col in ["general_firstup", "general_lastup"]:
        if col in df_in.columns:
            if pd.api.types.is_datetime64_any_dtype(df_in[col]):
                df_in[col] = df_in[col].dt.strftime('%Y-%m-%d').fillna("-")
            else:
                df_in[col] = df_in[col].astype(str).apply(lambda x: x.split(" ")[0])

    if "novelupdated_at" in df_in.columns:
        if pd.api.types.is_datetime64_any_dtype(df_in["novelupdated_at"]):
            df_in["novelupdated_at"] = df_in["novelupdated_at"].dt.strftime('%Y-%m-%d %H:%M').fillna("-")

    gb = GridOptionsBuilder.from_dataframe(df_in)
    gb.configure_default_column(sortable=False)
    gb.configure_selection(selection_mode='single', use_checkbox=False)
    gb.configure_grid_options(domLayout='normal')

    gb.configure_column("ncode", header_name="Nコード", width=150, sortable=True)
    gb.configure_column("title", header_name="タイトル", width=670, wrapText=True, autoHeight=True, sortable=True)
    gb.configure_column("writer", header_name="著者", width=150, sortable=True)
    
    gb.configure_column("genre", header_name="ジャンル", width=170, sortable=True)
    gb.configure_column("keyword", hide=True)
    gb.configure_column("general_firstup", header_name="初回掲載日", width=150, sortable=True)
    gb.configure_column("general_lastup", header_name="最終掲載日", width=150, sortable=True)
    
    gb.configure_column("general_all_no", header_name="話数", width=80, filter=False, sortable=True)
    gb.configure_column("length", hide=True)
    
    gb.configure_column("global_point", header_name="総合評価ポイント", width=190, filter=False, sortable=True)
    gb.configure_column("daily_point", header_name="日間ポイント", width=150, filter=False, sortable=True)
    gb.configure_column("weekly_point", hide=True)
    gb.configure_column("monthly_point", hide=True)
    
    gb.configure_column("fav_novel_cnt", hide=True)
    gb.configure_column("all_point", hide=True)
    
    gb.configure_column("novelupdated_at", header_name="作品の更新日時", width=220, sortable=True)
    
    gb.configure_column("weekly_unique", header_name="週間UU数", width=120, filter=False, sortable=True)
    gb.configure_column("classification", header_name="分類", hide=True)
    gb.configure_column("my_rating", header_name="評価（自）", width=120)
    gb.configure_column("other_ratings_text", header_name="評価（他）", width=250)
    gb.configure_column("my_comment", hide=True)

    for col in ["is_ng", "is_admin_evaluated", "is_admin_rejected", "is_general_evaluated", "is_general_rejected", "is_unclassified"]:
        gb.configure_column(col, hide=True)

    gridOptions = gb.build()
    
    grid_response = AgGrid(
        df_in,
        gridOptions=gridOptions,
        update_on=['selectionChanged'],
        fit_columns_on_grid_load=False,
        height=500,
        theme='streamlit',
        key=f'aggrid_{key_suffix}'
    )

    # ページネーションコントロール
    total_pages = (total_count // page_size) + (1 if total_count % page_size > 0 else 0)
    start_idx = (page - 1) * page_size
    
    if total_pages > 1:
        col_info, col_size, col_prev, col_page, col_next = st.columns([3, 2, 1, 2, 1])
        
        with col_info:
            st.caption(f"全 {total_count} 件中 {start_idx + 1} - {min(start_idx + page_size, total_count)} 件")

        with col_size:
            # size_key = f"page_size_{key_suffix}" # session_stateはmain_contentで管理
            pass 

        with col_prev:
            def prev_page():
                st.session_state[f"current_page_{key_suffix}"] -= 1
            
            st.button("前", key=f"prev_{key_suffix}", disabled=(page <= 1), use_container_width=True, on_click=prev_page)

        with col_page:
            st.markdown(
                f"<div style='text-align: center; line-height: 2.3;'>{page} / {total_pages}</div>",
                unsafe_allow_html=True
            )

        with col_next:
            def next_page():
                st.session_state[f"current_page_{key_suffix}"] += 1

            st.button("次", key=f"next_{key_suffix}", disabled=(page >= total_pages), use_container_width=True, on_click=next_page)

    selected = grid_response['selected_rows']
    if selected is not None and len(selected) > 0:
        if isinstance(selected, pd.DataFrame):
            return selected.iloc[0]['ncode']
        else:
            return selected[0].get('ncode')
    return None

def execute_search_query(conn, user_name, genre_label, filter_netcon14, search_keyword, exclude_keyword, min_global, max_global, sort_col, is_ascending, firstup_from, firstup_to, lastup_from, lastup_to, tab_filter, page, page_size):
    params = []
    params.append(user_name)

    query_select = """
        SELECT 
            t1.ncode, t1.title, t1.writer, t1.genre, t1.keyword,
            t1.general_firstup, t1.general_lastup, t1.general_all_no, t1.length,
            t1.global_point, t1.daily_point, t1.weekly_point, t1.monthly_point, 
            t1.fav_novel_cnt, t1.all_point, t1.novelupdated_at, t1.weekly_unique,
            
            COALESCE(t2.is_ng, FALSE) as is_ng,
            COALESCE(t2.is_admin_evaluated, FALSE) as is_admin_evaluated,
            COALESCE(t2.is_admin_rejected, FALSE) as is_admin_rejected,
            COALESCE(t2.is_general_evaluated, FALSE) as is_general_evaluated,
            COALESCE(t2.is_general_rejected, FALSE) as is_general_rejected,
            
            t3.rating as my_rating,
            t3.comment as my_comment
        FROM master_novels t1
        LEFT JOIN novel_status t2 ON t1.ncode = t2.ncode
        LEFT JOIN user_ratings_raw t3 ON t1.ncode = t3.ncode AND t3.user_name = ?
        WHERE 1=1
    """

    if genre_label != "すべて":
        target_code = next((k for k, v in GENRE_MAP.items() if v == genre_label), None)
        if target_code:
            query_select += " AND t1.genre = ?"
            params.append(target_code)

    if filter_netcon14:
        query_select += " AND (t1.keyword ILIKE '%ネトコン14%' OR t1.keyword ILIKE '%ネトコン１４%')"
        
    if search_keyword:
        keywords = search_keyword.replace("　", " ").split()
        for k in keywords:
            query_select += " AND (t1.title ILIKE ? OR t1.writer ILIKE ? OR t1.story ILIKE ? OR t1.keyword ILIKE ? OR t1.ncode ILIKE ?)"
            p = f"%{k}%"
            params.extend([p, p, p, p, p])

    if exclude_keyword:
        ex_keywords = exclude_keyword.replace("　", " ").split()
        for k in ex_keywords:
            query_select += " AND NOT (t1.title ILIKE ? OR t1.writer ILIKE ? OR t1.story ILIKE ? OR t1.keyword ILIKE ? OR t1.ncode ILIKE ?)"
            p = f"%{k}%"
            params.extend([p, p, p, p, p])

    if min_global > 0:
        query_select += " AND t1.global_point >= ?"
        params.append(min_global)
    if max_global > 0:
        query_select += " AND t1.global_point < ?"
        params.append(max_global)

    if firstup_from:
        query_select += " AND t1.general_firstup >= ?"
        params.append(pd.to_datetime(firstup_from))
    if firstup_to:
        query_select += " AND t1.general_firstup < ?"
        ts_to = pd.to_datetime(firstup_to) + timedelta(days=1)
        params.append(ts_to)

    if lastup_from:
        query_select += " AND t1.general_lastup >= ?"
        params.append(pd.to_datetime(lastup_from))
    if lastup_to:
        query_select += " AND t1.general_lastup < ?"
        ts_to = pd.to_datetime(lastup_to) + timedelta(days=1)
        params.append(ts_to)
        
    if tab_filter == "未評価":
        query_select += """ AND (
            t2.ncode IS NULL OR 
            (
                (t2.is_ng IS NULL OR t2.is_ng = FALSE) AND 
                (t2.is_admin_evaluated IS NULL OR t2.is_admin_evaluated = FALSE) AND
                (t2.is_admin_rejected IS NULL OR t2.is_admin_rejected = FALSE) AND
                (t2.is_general_evaluated IS NULL OR t2.is_general_evaluated = FALSE) AND
                (t2.is_general_rejected IS NULL OR t2.is_general_rejected = FALSE)
            )
        )"""
    elif tab_filter == "○／△（原作管理）":
        query_select += " AND t2.is_admin_evaluated = TRUE"
    elif tab_filter == "○／△（一般編集）":
        query_select += " AND t2.is_general_evaluated = TRUE"
    elif tab_filter == "×（原作管理）":
        query_select += " AND t2.is_admin_rejected = TRUE"
    elif tab_filter == "×（一般編集）":
        query_select += " AND t2.is_general_rejected = TRUE"
    elif tab_filter == "NG（商業化済み／原作管理判定）":
        query_select += " AND t2.is_ng = TRUE"

    count_sql = f"SELECT COUNT(*) FROM ({query_select}) AS sub"
    try:
        total_count = conn.execute(count_sql, params).fetchone()[0]
    except Exception as e:
        return pd.DataFrame(), 0

    if sort_col:
        safe_cols = ["global_point", "daily_point", "novelupdated_at", "ncode", "title", "writer", "genre", "general_firstup", "general_lastup", "general_all_no", "weekly_unique"]
        if sort_col in safe_cols:
            direction = "ASC" if is_ascending else "DESC"
            query_select += f" ORDER BY t1.{sort_col} {direction} NULLS LAST"
            
    if page_size > 0:
        offset = (page - 1) * page_size
        query_select += f" LIMIT {page_size} OFFSET {offset}"
        
    try:
        df = conn.execute(query_select, params).df()
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame(), 0
        
    if df.empty:
        return df, total_count

    df["genre"] = df["genre"].astype(str).map(GENRE_MAP).fillna(df["genre"])
    
    flag_cols = ["is_ng", "is_admin_evaluated", "is_admin_rejected", "is_general_evaluated", "is_general_rejected"]
    for c in flag_cols:
        if c not in df.columns:
            df[c] = False
    
    df["is_unclassified"] = ~df[flag_cols].any(axis=1)
    
    def get_disp(row):
        if row["is_ng"]: return "NG"
        if row["is_admin_evaluated"]: return "Admin〇△"
        if row["is_admin_rejected"]: return "Admin×"
        if row["is_general_evaluated"]: return "Gen〇△"
        if row["is_general_rejected"]: return "Gen×"
        return "-"
    df["classification"] = df.apply(get_disp, axis=1)

    df_all = load_all_ratings_table()
    if not df_all.empty:
        target_ncodes = df["ncode"].tolist()
        others = df_all[
            (df_all["ncode"].isin(target_ncodes)) & 
            (df_all["user_name"] != user_name) & 
            (df_all["rating"].notna()) & 
            (df_all["rating"] != "")
        ].copy()
        
        if not others.empty:
            others["_temp"] = others["user_name"] + ":" + others["rating"]
            agg = others.groupby("ncode")["_temp"].agg(" ".join).reset_index()
            agg.columns = ["ncode", "other_ratings_text"]
            df = pd.merge(df, agg, on="ncode", how="left")
    
    if "other_ratings_text" not in df.columns:
        df["other_ratings_text"] = None
        
    return df, total_count

@st.fragment
def main_content(user_name):
    if not st.session_state.get("data_loaded", False):
        return
    
    target_col = sort_map.get(sort_col_label) if sort_col_label else None
    ascending = (sort_order == "昇順")

    tab_options = [
        "すべて", 
        "未評価", 
        "○／△（原作管理）", 
        "○／△（一般編集）", 
        "×（原作管理）", 
        "×（一般編集）", 
        "NG（商業化済み／原作管理判定）"
    ]

    st.markdown("""
    <style>
        div[role="radiogroup"] {
            background-color: transparent;
            border-bottom: 2px solid #f0f2f6;
            padding-bottom: 0px;
            gap: 0px;
        }
        div[role="radiogroup"] > label {
            background-color: transparent !important;
            border: 1px solid transparent;
            border-radius: 5px 5px 0 0;
            padding: 0.5rem 1rem;
            margin-right: 2px;
            margin-bottom: -2px;
            transition: all 0.2s;
        }
        div[role="radiogroup"] > label:hover {
            background-color: #f8f9fa !important;
            color: #ff4b4b;
        }
        div[role="radiogroup"] > label > div:first-child {
            display: none !important;
        }
        div[role="radiogroup"] label[data-baseweb="radio"] {
            padding: 0.5rem 1rem;
            border-bottom: 2px solid transparent;
        }
        div[role="radiogroup"] label[data-baseweb="radio"] > div {
            font-weight: 500;
        }
        div[role="radiogroup"] label:has(input:checked) {
            border-bottom: 3px solid #ff4b4b !important;
            color: #ff4b4b;
            background-color: #fff;
        }
    </style>
    """, unsafe_allow_html=True)

    current_tab = st.radio(
        "表示切り替え",
        tab_options,
        horizontal=True,
        label_visibility="collapsed",
        key="selected_tab_nav"
    )

    # キー作成
    key_suffix = "all"
    if current_tab == "未評価": key_suffix = "unclassified"
    elif current_tab == "○／△（原作管理）": key_suffix = "evaluated_team"
    elif current_tab == "○／△（一般編集）": key_suffix = "evaluated_edit"
    elif current_tab == "×（原作管理）": key_suffix = "rejected_team"
    elif current_tab == "×（一般編集）": key_suffix = "rejected_edit"
    elif current_tab == "NG（商業化済み／原作管理判定）": key_suffix = "ng_commercialized"

    # ページング状態
    page_key = f"current_page_{key_suffix}"
    if page_key not in st.session_state:
        st.session_state[page_key] = 1
        
    page = st.session_state[page_key]
    page_size = 300 # 固定

    # データ取得
    df, total_count = execute_search_query(
        conn,
        user_name, 
        genre, 
        filter_netcon14, 
        search_keyword, 
        exclude_keyword, 
        min_global, 
        max_global, 
        target_col, 
        ascending,
        firstup_from,
        firstup_to,
        lastup_from,
        lastup_to,
        current_tab,
        page,
        page_size
    )
    
    # ローカルパッチ適用
    df = apply_local_patches(df, user_name)

    selected_ncode = render_novel_list(df, total_count, key_suffix, page, page_size)

    # ==================================================
    # 下：編集 + 詳細
    # ==================================================

    if selected_ncode is None:
        st.info("作品を一覧から選択してください")
        return

    # 詳細表示のために再度取得、あるいは df から取得
    # df に含まれているはず
    row_df = df[df["ncode"] == selected_ncode]
    if row_df.empty:
        # ページ遷移などで消えた場合、別途取得するかエラー
        st.error("データが見つかりません")
        return

    row = row_df.iloc[0]

    def fmt_num(val, unit=""):
        try:
            if pd.isna(val) or val == "": return "-"
            num = float(val)
            if num.is_integer():
                    return f"{int(num):,}{unit}"
            return f"{num:,}{unit}"
        except:
            return str(val)

    with st.container(border=True):
        st.markdown(f"## {row['title']}")
        
        narou_url = f"https://ncode.syosetu.com/{row['ncode'].lower()}/"
        google_url = f"https://www.google.com/search?q={row['title']}"

        st.markdown(f"""
        <div style="margin-bottom: 5px;">
            <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 12px; margin-bottom: 8px;">
                <div style="color: #666; font-size: 0.9rem;">
                    著者: <b>{row.get('writer', '不明')}</b>
                    <span style="margin: 0 8px; color: #ddd;">|</span>
                    Nコード: {row['ncode']}
                    <span style="margin: 0 8px; color: #ddd;">|</span>
                    初回掲載日: {str(row.get('general_firstup', '-')).split(' ')[0]}
                    <span style="margin: 0 8px;"></span>
                    最終掲載日: {str(row.get('general_lastup', '-')).split(' ')[0]}
                </div>
            </div>
            <div style="display: flex; gap: 10px;">
                <a href="{narou_url}" target="_blank" rel="noopener noreferrer" style="text-decoration: none;">
                    <div style="display: inline-flex; align-items: center; padding: 4px 12px; background-color: #eef2f6; border-radius: 15px; color: #2c3e50; font-size: 0.8rem; font-weight: 500; border: 1px solid #dae1e7; transition: all 0.2s;">
                        本文を読む
                    </div>
                </a>
                <a href="{google_url}" target="_blank" rel="noopener noreferrer" style="text-decoration: none;">
                    <div style="display: inline-flex; align-items: center; padding: 4px 12px; background-color: #fff; border-radius: 15px; color: #5f6368; font-size: 0.8rem; font-weight: 500; border: 1px solid #dae1e7; transition: all 0.2s;">
                        Google
                    </div>
                </a>
            </div>
        </div>
        <hr style="border: 0; border-top: 2px solid #f0f2f6; margin: 20px 0;">
        """, unsafe_allow_html=True)



        col_left, col_right = st.columns([1, 2], gap="large")

        with col_left:
            st.markdown(f"""
            <div style="margin-bottom: 10px;">
                <div class="label">ジャンル</div>
                <div class="value" style="color: #3498db; font-size: 1rem;">{row.get('genre', '-')}</div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown(f"""
            <div style="margin-bottom: 20px;">
                <div class="label">タグ</div>
                <div style="font-size: 0.85rem; color: #666; line-height: 1.4;">{row.get('keyword', '-')}</div>
            </div>
            """, unsafe_allow_html=True)

            c1, c2, c3, c4 = st.columns([1, 1, 1, 1.2], gap="small")

            with c1:
                st.markdown(f"""
                <div class="label">総合評価</div>
                <div class="value" style="font-size: 1.0rem; margin-bottom: 10px;">{fmt_num(row.get('global_point'), 'pt')}</div>
                """, unsafe_allow_html=True)
                
            with c2:
                st.markdown(f"""
                <div class="label">エピソード数</div>
                <div class="value" style="font-size: 1.0rem; margin-bottom: 10px;">{fmt_num(row.get('general_all_no'), '話')}</div>
                """, unsafe_allow_html=True)

            with c3:
                st.markdown(f"""
                <div class="label">文字数</div>
                <div class="value" style="font-size: 1.0rem; margin-bottom: 10px;">{fmt_num(row.get('length'), '文字')}</div>
                """, unsafe_allow_html=True)

            with c4:
                with st.expander("その他統計"):
                    st.markdown(f"""
                    <div style="font-size: 0.8rem; line-height: 1.6; color: #555;">
                        <div style="display:flex; justify-content:space-between; border-bottom:1px solid #eee;"><span>評価</span><b>{fmt_num(row.get('all_point'))}</b></div>
                        <div style="display:flex; justify-content:space-between; border-bottom:1px solid #eee;"><span>Bookmark</span><b>{fmt_num(row.get('fav_novel_cnt'))}</b></div>
                        <div style="display:flex; justify-content:space-between; border-bottom:1px solid #eee;"><span>日間pt</span><b>{fmt_num(row.get('daily_point'))}</b></div>
                        <div style="display:flex; justify-content:space-between; border-bottom:1px solid #eee;"><span>週間pt</span><b>{fmt_num(row.get('weekly_point'))}</b></div>
                        <div style="display:flex; justify-content:space-between; border-bottom:1px solid #eee;"><span>月間pt</span><b>{fmt_num(row.get('monthly_point'))}</b></div>
                        <div style="display:flex; justify-content:space-between;"><span>週間UU</span><b>{fmt_num(row.get('weekly_unique'))}</b></div>
                    </div>
                    """, unsafe_allow_html=True)

            st.markdown(f"""
            <div style="margin-bottom: 10px;">
            """, unsafe_allow_html=True)
            
            st.markdown('<div class="label">評価アクション</div>', unsafe_allow_html=True)
            
            initial_comment = row.get("my_comment")
            if pd.isna(initial_comment): initial_comment = ""
            
            role = "原作管理チーム" if user_name in ADMIN_TEAM_USERS else "一般編集"

            col_btn1, col_btn2 = st.columns(2)
            col_btn3, col_btn4 = st.columns(2)

            current_my_rating = row.get("my_rating")
            if pd.isna(current_my_rating):
                current_my_rating = None

            with col_btn1:
                btn_type = "primary" if current_my_rating == "〇" else "secondary"
                st.button(
                    "○ 面白い／コミカライズし易そう", 
                    type=btn_type, 
                    use_container_width=True, 
                    key="btn_good",
                    on_click=on_rating_button_click,
                    args=(row['ncode'], user_name, "〇", current_my_rating, role)
                )
            
            with col_btn2:
                btn_type = "primary" if current_my_rating == "△" else "secondary"
                st.button(
                    "△ 保留", 
                    type=btn_type, 
                    use_container_width=True, 
                    key="btn_hold",
                    on_click=on_rating_button_click,
                    args=(row['ncode'], user_name, "△", current_my_rating, role)
                )

            with col_btn3:
                btn_type = "primary" if current_my_rating == "×" else "secondary"
                st.button(
                    "× 面白くない／しづらそう", 
                    type=btn_type, 
                    use_container_width=True, 
                    key="btn_bad",
                    on_click=on_rating_button_click,
                    args=(row['ncode'], user_name, "×", current_my_rating, role)
                )

            with col_btn4:
                ng_label = "NG（商業化済み／原作管理判定）" if role == "原作管理チーム" else "NG（商業化済み）"
                
                btn_type = "primary" if current_my_rating == "NG" else "secondary"
                st.button(
                    ng_label, 
                    type=btn_type, 
                    use_container_width=True, 
                    key="btn_ng",
                    on_click=on_rating_button_click,
                    args=(row['ncode'], user_name, "NG", current_my_rating, role)
                )

            def on_comment_change():
                new_comment = st.session_state[f"input_comment_area_{row['ncode']}"]
                role_tmp = "原作管理チーム" if user_name in ADMIN_TEAM_USERS else "一般編集"
                save_comment_only(row['ncode'], user_name, new_comment, role_tmp)

            input_comment = st.text_area(
                "コメント", 
                value=initial_comment, 
                height=100, 
                key=f"input_comment_area_{row['ncode']}",
                on_change=on_comment_change
            )



        with col_right:
            st.markdown('<div class="label" style="margin-bottom: 8px;">あらすじ</div>', unsafe_allow_html=True)
            
            story_text = load_novel_story(row['ncode'])
            
            st.markdown(f"""
            <div class="story-box" style="margin-bottom: 30px;">
            {story_text.replace('\n', '<br>')}
            </div>
            """, unsafe_allow_html=True)

            st.subheader("評価者一覧")
            other_ratings_df = load_novel_ratings_all(row['ncode'])

            if "local_rating_patches" in st.session_state and row['ncode'] in st.session_state["local_rating_patches"]:
                patch = st.session_state["local_rating_patches"][row['ncode']]
                
                new_row = {
                    "ncode": row['ncode'],
                    "user_name": user_name,
                    "rating": patch["rating"],
                    "comment": patch["comment"],
                    "role": patch["role"],
                    "updated_at": patch["updated_at"]
                }
                
                if other_ratings_df.empty:
                    other_ratings_df = pd.DataFrame([new_row])
                else:
                    my_idx = other_ratings_df[other_ratings_df["user_name"] == user_name].index
                    if not my_idx.empty:
                        for k, v in new_row.items():
                            other_ratings_df.loc[my_idx, k] = v
                    else:
                        other_ratings_df = pd.concat([other_ratings_df, pd.DataFrame([new_row])], ignore_index=True)

            if not other_ratings_df.empty:
                r_check = other_ratings_df["rating"].fillna("").astype(str).str.strip().replace("None", "")
                c_check = other_ratings_df["comment"].fillna("").astype(str).str.strip().replace("None", "")
                
                disp_ratings = other_ratings_df[(r_check != "") | (c_check != "")].copy()
                
                if disp_ratings.empty:
                    st.info("まだ評価はありません")
                else:
                    if 'updated_at' in disp_ratings.columns:
                        disp_ratings['updated_at'] = pd.to_datetime(disp_ratings['updated_at'], utc=True, errors='coerce').dt.tz_convert('Asia/Tokyo').dt.strftime('%Y-%m-%d %H:%M')

                    
                    target_cols = ['user_name', 'rating', 'comment', 'updated_at']
                    disp_ratings = disp_ratings[[c for c in target_cols if c in disp_ratings.columns]]
                    
                    rename_map = {
                        'user_name': '名前',
                        'rating': '評価',
                        'comment': 'コメント',
                        'updated_at': '日時'
                    }
                    disp_ratings = disp_ratings.rename(columns=rename_map)

                    st.dataframe(
                        disp_ratings, 
                        hide_index=True, 
                        use_container_width=True, 
                        column_config={
                            "名前": st.column_config.TextColumn(width="small"),
                            "評価": st.column_config.TextColumn(width="small"),
                            "コメント": st.column_config.TextColumn(width="large"),
                            "日時": st.column_config.TextColumn(width="small"),
                        }
                    )
            else:
                st.info("まだ評価はありません")

    st.write("")        

main_content(user_name)
