import streamlit as st
import pandas as pd
import gspread
import json
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# ==================================================
# 定数定義
# ==================================================
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

@st.cache_data(ttl=7200)
def load_master_data():
    service_account_info = json.loads(
        st.secrets["gcp"]["service_account_json"]
    )
    gc = gspread.service_account_from_dict(service_account_info)
    sheet = gc.open_by_url(
        st.secrets["gcp"]["sheet_url"]
    ).sheet1

    data = sheet.get_all_values()
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data[1:], columns=data[0])

    if "genre" in df.columns:
        df["genre"] = df["genre"].astype(str).map(GENRE_MAP).fillna(df["genre"])

    numeric_cols = ["global_point", "daily_point", "weekly_point", "monthly_point", 
                   "quarter_point", "yearly_point", "all_point", "general_all_no", 
                   "weekly_unique", "fav_novel_cnt", "impression_cnt", "review_cnt", "sasie_cnt", "kaiwaritu"]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df

@st.cache_data(ttl=300)
def load_user_ratings(user_name):
    res = (
        supabase.table("user_ratings")
        .select("*")
        .eq("user_name", user_name)
        .execute()
    )
    return pd.DataFrame(res.data)

@st.cache_data(ttl=60)
def load_all_ratings_table():
    """全ユーザーの評価を取得（分類用）"""
    res = supabase.table("user_ratings").select("*").execute()
    return pd.DataFrame(res.data)

@st.cache_data(ttl=60)
def load_novel_ratings_all(ncode):
    """特定作品の全ユーザー評価を取得"""
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
    """JSTの現在時刻を取得してISOフォーマット文字列で返す"""
    JST = timezone(timedelta(hours=9), 'JST')
    return datetime.now(JST).isoformat()


def save_rating(ncode, user_name, rating, comment, role):
    """評価を保存"""
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
    """評価ボタン押下時のコールバック"""
    comment = st.session_state.get(f"input_comment_area_{ncode}", "")
    
    new_rating = None if current_rating == target_rating else target_rating
    
    save_rating(ncode, user_name, new_rating, comment, role)

def save_comment_only(ncode, user_name, comment, role):
    """コメントのみ保存（評価は維持）"""
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
    """
    データフレーム（特定作品の評価一覧）からステータスフラグを判定する
    """
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
    """
    全評価データから作品ごとの分類ステータスを算出
    優先度: NG > 原作管理× > ○ > △ > ×
    """
    if df_ratings.empty:
        return pd.DataFrame()

    if "role" not in df_ratings.columns:
        pass

    results = []
    
    for ncode, group in df_ratings.groupby("ncode"):
        flags = determine_status(group)
        row = {"ncode": ncode}
        row.update(flags)
        results.append(row)
        
    return pd.DataFrame(results)



@st.cache_data(ttl=60)
def get_processed_novel_data(user_name):
    """
    表示用データの生成（キャッシュ化）
    Fragmentのリラン時に高速に応答するために、重い処理（結合・計算）をキャッシュする。
    評価更新時はこのキャッシュをクリアする。
    """
    df_master = load_master_data()
    df_ratings = load_user_ratings(user_name)
    df_all_ratings_raw = load_all_ratings_table()
    
    df_classification = calculate_novel_status(df_all_ratings_raw)

    evaluated_ncodes = []

    if not df_classification.empty:
        df = pd.merge(df_master, df_classification, on="ncode", how="left")
        flag_cols = ["is_ng", "is_admin_evaluated", "is_admin_rejected", "is_general_evaluated", "is_general_rejected", "is_unclassified"]
        
        for col in flag_cols:
            if col not in df.columns:
                df[col] = False
            else:
                df[col] = df[col].fillna(False)

        evaluated_ncodes = df_classification["ncode"].unique()
        df.loc[~df["ncode"].isin(evaluated_ncodes), "is_unclassified"] = True
        
    else:
        df = df_master.copy()
        df["is_ng"] = False
        df["is_admin_evaluated"] = False
        df["is_admin_rejected"] = False
        df["is_general_evaluated"] = False
        df["is_general_rejected"] = False
        df["is_unclassified"] = True
        
    if not df_ratings.empty:
        my_ratings = df_ratings[["ncode", "rating", "comment"]].rename(
            columns={"rating": "my_rating", "comment": "my_comment"}
        )
        df = pd.merge(df, my_ratings, on="ncode", how="left")
    else:
        df["my_rating"] = None
        df["my_comment"] = None

    if not df_all_ratings_raw.empty:
        others_df = df_all_ratings_raw[
            (df_all_ratings_raw["user_name"] != user_name) & 
            (df_all_ratings_raw["rating"].notna()) & 
            (df_all_ratings_raw["rating"] != "")
        ].copy()
        
        if not others_df.empty:
            others_df["_temp_summary"] = others_df["user_name"] + ":" + others_df["rating"]
            others_agg = others_df.groupby("ncode")["_temp_summary"].apply(lambda x: " ".join(x)).reset_index()
            others_agg.columns = ["ncode", "other_ratings_text"]
            df = pd.merge(df, others_agg, on="ncode", how="left")
    
    if "other_ratings_text" not in df.columns:
        df["other_ratings_text"] = None

    if len(evaluated_ncodes) > 0:
        df.loc[~df["ncode"].isin(evaluated_ncodes), "is_unclassified"] = True
    elif df_classification.empty:
        df["is_unclassified"] = True

    def get_disp_status(row):
        if row["is_ng"]: return "NG"
        if row["is_admin_evaluated"]: return "Admin〇△"
        if row["is_admin_rejected"]: return "Admin×"
        if row["is_general_evaluated"]: return "Gen〇△"
        if row["is_general_rejected"]: return "Gen×"
        return "-"

    df["classification"] = df.apply(get_disp_status, axis=1)
    
    return df


def apply_local_patches(df, user_name):
    """
    キャッシュされたデータフレームに対し、ローカル（session_state）上の未反映パッチを適用する
    """
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
</style>
""", unsafe_allow_html=True)

st.title("なろう小説 ダッシュボード")

with st.spinner("データ読み込み中…"):
    df_master = load_master_data()

if df_master.empty:
    st.error("マスタデータが取得できません")
    st.stop()

# ==================================================
# フィルタ
# ==================================================
st.sidebar.header("絞り込み")
st.sidebar.caption("初回投稿日が2024年2月1日以降かつ第14回ネット小説大賞の投稿作のみ表示中")

genres = ["すべて"]
if "genre" in df_master.columns:
    existing_genres = set(df_master["genre"].dropna().unique())
    
    sorted_genres = []
    for g_val in GENRE_MAP.values():
        if g_val in existing_genres:
            sorted_genres.append(g_val)
    
    others = sorted(list(existing_genres - set(sorted_genres)))
    
    genres += sorted_genres + others

genre = st.sidebar.selectbox("ジャンル", genres)

search_keyword = st.sidebar.text_input("キーワード検索")
exclude_keyword = st.sidebar.text_input("検索除外ワード")

st.sidebar.markdown("---")
st.sidebar.caption("ポイントフィルタ")
min_global = st.sidebar.number_input("総合ポイント 以上", min_value=0, value=0, step=1000)
max_global = st.sidebar.number_input("総合ポイント 未満", min_value=0, value=0, step=1000)

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

sort_map = {k: v for k, v in sort_map.items() if v in df_master.columns}

default_sort_index = 0
if "日間ポイント" in sort_map:
    default_sort_index = list(sort_map.keys()).index("日間ポイント")
elif "総合評価ポイント" in sort_map:
    default_sort_index = list(sort_map.keys()).index("総合評価ポイント")

sort_col_label = st.sidebar.selectbox("ソート項目", list(sort_map.keys()), index=default_sort_index)
sort_order = st.sidebar.radio("順序", ["降順", "昇順"], index=0)          

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


# ==================================================
# リスト表示関数
# ==================================================
def render_novel_list(df_in, key_suffix):
    if df_in.empty:
        st.info("表示対象のデータがありません")
        return None

    page_key = f"current_page_{key_suffix}"
    size_key = f"page_size_{key_suffix}"

    if page_key not in st.session_state:
        st.session_state[page_key] = 1
    if size_key not in st.session_state:
        st.session_state[size_key] = 300

    PAGE_SIZE = st.session_state[size_key]

    total_count = len(df_in)
    total_pages = (total_count // PAGE_SIZE) + (1 if total_count % PAGE_SIZE > 0 else 0)

    if st.session_state[page_key] > total_pages:
        st.session_state[page_key] = 1

    start_idx = (st.session_state[page_key] - 1) * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    display_df = df_in.iloc[start_idx:end_idx].copy()

    for col in ["general_firstup", "general_lastup"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].astype(str).apply(lambda x: x.split(" ")[0])

    gb = GridOptionsBuilder.from_dataframe(display_df)
    gb.configure_default_column(sortable=False)
    gb.configure_selection(selection_mode='single', use_checkbox=False)
    gb.configure_grid_options(domLayout='normal')

    gb.configure_column("ncode", header_name="Nコード", width=150, sortable=True)
    gb.configure_column("title", header_name="タイトル", width=67670, wrapText=True, autoHeight=True, sortable=True)
    gb.configure_column("userid", hide=True)
    gb.configure_column("writer", header_name="著者", width=150, sortable=True)
    gb.configure_column("story", hide=True)
    gb.configure_column("biggenre", hide=True)
    gb.configure_column("genre", header_name="ジャンル", width=170, sortable=True)
    gb.configure_column("gensaku", hide=True)
    gb.configure_column("keyword", hide=True)
    gb.configure_column("general_firstup", header_name="初回掲載日", width=150, sortable=True)
    gb.configure_column("general_lastup", header_name="最終掲載日", width=150, sortable=True)
    gb.configure_column("novel_type", hide=True)
    gb.configure_column("end", hide=True)
    gb.configure_column("general_all_no", header_name="話数", width=80, filter=False, sortable=True)
    gb.configure_column("length", hide=True)
    gb.configure_column("time", hide=True)
    gb.configure_column("isstop", hide=True)
    gb.configure_column("isr15", hide=True)
    gb.configure_column("isbl", hide=True)
    gb.configure_column("isgl", hide=True)
    gb.configure_column("iszankoku", hide=True)
    gb.configure_column("istensei", hide=True)
    gb.configure_column("istenni", hide=True)
    gb.configure_column("global_point", header_name="総合評価ポイント", width=190, filter=False, sortable=True)
    gb.configure_column("daily_point", header_name="日間ポイント", width=150, filter=False, sortable=True)
    gb.configure_column("weekly_point", hide=True)
    gb.configure_column("monthly_point", hide=True)
    gb.configure_column("quarter_point", hide=True)
    gb.configure_column("yearly_point", hide=True)
    gb.configure_column("fav_novel_cnt", hide=True)
    gb.configure_column("impression_cnt", hide=True)
    gb.configure_column("review_cnt", hide=True)
    gb.configure_column("all_point", hide=True)
    gb.configure_column("all_hyoka_cnt", hide=True)
    gb.configure_column("sasie_cnt", hide=True)
    gb.configure_column("kaiwaritu", hide=True)
    gb.configure_column("novelupdated_at", header_name="作品の更新日時", width=220, sortable=True)
    gb.configure_column("updated_at", hide=True)
    gb.configure_column("weekly_unique", header_name="週間UU数", width=120, filter=False, sortable=True)
    gb.configure_column("classification", header_name="分類", hide=True)
    gb.configure_column("my_rating", header_name="評価（自）", width=120)
    gb.configure_column("other_ratings_text", header_name="評価（他）", width=250)
    gb.configure_column("my_comment", hide=True)

    for col in ["is_ng", "is_admin_evaluated", "is_admin_rejected", "is_general_evaluated", "is_general_rejected", "is_unclassified"]:
        gb.configure_column(col, hide=True)

    gridOptions = gb.build()

    
    grid_response = AgGrid(
        display_df,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=False,
        height=500,
        theme='streamlit',
        key=f'aggrid_{key_suffix}'
    )

    if total_pages > 1:
        col_info, col_size, col_prev, col_page, col_next = st.columns([3, 2, 1, 2, 1])
        
        with col_info:
            st.caption(f"全 {total_count} 件中 {start_idx + 1} - {min(end_idx, total_count)} 件")

        with col_size:
            try:
                current_idx = [100, 300, 500].index(st.session_state[size_key])
            except ValueError:
                current_idx = 1
            
            def on_size_change():
                st.session_state[size_key] = st.session_state[f"size_sel_{key_suffix}"]
                st.session_state[page_key] = 1

            st.selectbox(
                "表示件数", 
                [100, 300, 500], 
                index=current_idx,
                key=f"size_sel_{key_suffix}",
                label_visibility="collapsed",
                on_change=on_size_change
            )

        with col_prev:
            def prev_page():
                st.session_state[page_key] -= 1
            
            st.button("前", key=f"prev_{key_suffix}", disabled=(st.session_state[page_key] <= 1), use_container_width=True, on_click=prev_page)

        with col_page:
            st.markdown(
                f"<div style='text-align: center; line-height: 2.3;'>{st.session_state[page_key]} / {total_pages}</div>",
                unsafe_allow_html=True
            )

        with col_next:
            def next_page():
                st.session_state[page_key] += 1

            st.button("次", key=f"next_{key_suffix}", disabled=(st.session_state[page_key] >= total_pages), use_container_width=True, on_click=next_page)

    selected = grid_response['selected_rows']
    if selected is not None and len(selected) > 0:
        if isinstance(selected, pd.DataFrame):
            return selected.iloc[0]['ncode']
        else:
            return selected[0].get('ncode')
    return None

# ==================================================
# タブによるリスト切り替え
# ==================================================
def get_filtered_sorted_data(user_name, genre, search_keyword, exclude_keyword, min_global, max_global, sort_col, is_ascending):
    """
    フィルタリングとソートを行ったデータフレームを返す
    get_processed_novel_data（キャッシュ） + ローカルパッチ適用
    """
    df_base = get_processed_novel_data(user_name)
    
    df = apply_local_patches(df_base, user_name)
    
    if df is df_base:
        df = df.copy()

    # ==================================================
    # マスト条件: 「ネトコン14」を含む かつ 2024年2月1日以降
    # ==================================================

    if "general_firstup" in df.columns:
        temp_date = pd.to_datetime(df["general_firstup"], errors='coerce')
        df = df[temp_date >= "2024-02-01"]

    if genre != "すべて":
        df = df[df["genre"] == genre]

    if search_keyword:
        keywords = search_keyword.replace("　", " ").split()
        for k in keywords:
            mask = (
                df["title"].fillna("").astype(str).str.contains(k, case=False, na=False) |
                df["writer"].fillna("").astype(str).str.contains(k, case=False, na=False) |
                df["story"].fillna("").astype(str).str.contains(k, case=False, na=False) |
                df["keyword"].fillna("").astype(str).str.contains(k, case=False, na=False)
            )
            df = df[mask]

    if exclude_keyword:
        exclude_keywords = exclude_keyword.replace("　", " ").split()
        for k in exclude_keywords:
            mask_exclude = (
                df["title"].fillna("").astype(str).str.contains(k, case=False, na=False) |
                df["writer"].fillna("").astype(str).str.contains(k, case=False, na=False) |
                df["story"].fillna("").astype(str).str.contains(k, case=False, na=False) |
                df["keyword"].fillna("").astype(str).str.contains(k, case=False, na=False)
            )
            df = df[~mask_exclude]

    if min_global is not None and min_global > 0:
        df = df[df["global_point"] >= min_global]
    if max_global is not None and max_global > 0:
        df = df[df["global_point"] < max_global]

    if sort_col and sort_col in df.columns:
        df = df.sort_values(by=sort_col, ascending=is_ascending, na_position='last')
        
    return df

@st.fragment
def main_content(user_name):
    target_col = sort_map.get(sort_col_label) if sort_col_label else None
    ascending = (sort_order == "昇順")

    df = get_filtered_sorted_data(
        user_name, 
        genre, 
        search_keyword, 
        exclude_keyword, 
        min_global, 
        max_global, 
        target_col, 
        ascending
    )

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
        /* ラジオボタンのコンテナ */
        div[role="radiogroup"] {
            background-color: transparent;
            border-bottom: 2px solid #f0f2f6;
            padding-bottom: 0px;
            gap: 0px;
        }
        
        /* ラジオボタンの各アイテム（ラベル） */
        div[role="radiogroup"] > label {
            background-color: transparent !important;
            border: 1px solid transparent;
            border-radius: 5px 5px 0 0;
            padding: 0.5rem 1rem;
            margin-right: 2px;
            margin-bottom: -2px; /* 下線に重ねる */
            transition: all 0.2s;
        }

        /* ホバー時 */
        div[role="radiogroup"] > label:hover {
            background-color: #f8f9fa !important;
            color: #ff4b4b;
        }

        /* 丸ポチを非表示にする */
        div[role="radiogroup"] > label > div:first-child {
            display: none !important;
        }
        
        /* 選択された項目のスタイル（StreamlitのHTML構造に依存） 
           checked状態のinputの親labelに対するスタイル適用はCSSだけでは完全には難しいが、
           Streamlitはcheckedのdivにスタイルを当てていることがあるため、
           背景色やテキスト色で強調を試みる
        */
        div[role="radiogroup"] label[data-baseweb="radio"] {
            padding: 0.5rem 1rem;
            border-bottom: 2px solid transparent; /* デフォルトは透明な下線 */
        }

        /* 選択中の項目（背景色が変わる要素の中のテキスト） */
        div[role="radiogroup"] label[data-baseweb="radio"] > div {
            font-weight: 500;
        }

        /* 
           重要: Streamlitのラジオボタンは構造が複雑で、CSSの:has()対応ブラウザなら
           label:has(input:checked) でいけるが、Streamlitはinputを隠蔽していることが多い。
           しかし、標準的なスタイルでは選択されたアイテムのテキスト色がプライマリカラーになるため、
           それを利用して下線に見えるようなborderを追加するトリックを使う。
        */
        
        div[role="radiogroup"] label:has(input:checked) {
            border-bottom: 3px solid #ff4b4b !important; /* Streamlitの赤色 */
            color: #ff4b4b;
            background-color: #fff;
        }
        
        /* :has非対応環境へのフォールバック（完全ではないが、文字色等は変わる） */
        div[role="radiogroup"] input:checked + div {
            /* ここにスタイルを当てられると良いが構造上難しい場合がある */
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

    selected_ncode = None

    if current_tab == "すべて":
        ncode = render_novel_list(df, "all")
        if ncode: selected_ncode = ncode

    elif current_tab == "未評価":
        target = df[df["is_unclassified"]]
        ncode = render_novel_list(target, "unclassified")
        if ncode: selected_ncode = ncode

    elif current_tab == "○／△（原作管理）":
        target = df[df["is_admin_evaluated"]]
        ncode = render_novel_list(target, "evaluated_team")
        if ncode: selected_ncode = ncode

    elif current_tab == "○／△（一般編集）":
        target = df[df["is_general_evaluated"]]
        ncode = render_novel_list(target, "evaluated_edit")
        if ncode: selected_ncode = ncode

    elif current_tab == "×（原作管理）":
        target = df[df["is_admin_rejected"]]
        ncode = render_novel_list(target, "rejected_team")
        if ncode: selected_ncode = ncode

    elif current_tab == "×（一般編集）":
        target = df[df["is_general_rejected"]]
        ncode = render_novel_list(target, "rejected_edit")
        if ncode: selected_ncode = ncode

    elif current_tab == "NG（商業化済み／原作管理判定）":
        target = df[df["is_ng"]]
        ncode = render_novel_list(target, "ng_commercialized")
        if ncode: selected_ncode = ncode

    # ==================================================
    # 下：編集 + 詳細
    # ==================================================

    if selected_ncode is None:
        st.info("作品を一覧から選択してください")
        return

    row_df = df[df["ncode"] == selected_ncode]
    if row_df.empty:
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
                    初回掲載日: {row.get('general_firstup', '-').split(' ')[0]}
                    <span style="margin: 0 8px;"></span>
                    最終掲載日: {row.get('general_lastup', '-').split(' ')[0]}
                </div>
            </div>
            <div style="display: flex; gap: 10px;">
                <a href="{narou_url}" target="_blank" style="text-decoration: none;">
                    <div style="display: inline-flex; align-items: center; padding: 4px 12px; background-color: #eef2f6; border-radius: 15px; color: #2c3e50; font-size: 0.8rem; font-weight: 500; border: 1px solid #dae1e7; transition: all 0.2s;">
                        本文を読む
                    </div>
                </a>
                <a href="{google_url}" target="_blank" style="text-decoration: none;">
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
                key=f"input_comment_area_{row['ncode']}",\
                on_change=on_comment_change
            )



        with col_right:
            st.markdown('<div class="label" style="margin-bottom: 8px;">あらすじ</div>', unsafe_allow_html=True)
            st.markdown(f"""
            <div class="story-box" style="margin-bottom: 30px;">
            {row.get("story", "情報なし").replace('\n', '<br>')}
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
                disp_ratings = other_ratings_df.copy()
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
