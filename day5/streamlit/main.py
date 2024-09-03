import os
import streamlit as st
from snowflake.snowpark.context import get_active_session

# 定数定義
USER_NAME = "user"
ASSISTANT_NAME = "assistant"

@st.cache_resource
def create_execute_query():
    """
    クエリを実行するための関数execute_queryを一度だけ生成してキャッシュするための
    クロージャ
    """
    # セッションの取得
    session = get_active_session()

    def execute_query(stmts, params):
        return session.sql(stmts, params=params).to_pandas()      

    return execute_query

execute_query = create_execute_query()


def generate_response(
    user_msg: str,
):
    """返事を生成する"""
    retrieve_related_items = """
        with similar_items as (
            select
                item_id,
                VECTOR_COSINE_SIMILARITY(embedded_items.embedded_item_caption,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', ?)) as similarity,
            from embedded_items
            order by similarity desc
            limit ?
        ),
        
        distinct_similar_items as (
            select distinct item_id from similar_items
        ),

        final as (
            select
                t2.item_url,
                t2.item_caption
            from
                distinct_similar_items t1
                inner join SHARED_DB.RAKUTEN_EC_RAW.item t2
                on t1.item_id = t2.item_id
        )
        select * from final
    """
    
    df_context = execute_query(retrieve_related_items, params=[user_msg, 3]) 

    prompt_context_items = df_context["ITEM_CAPTION"].to_list()
    prompt_context = ", ".join(prompt_context_items)
    
    prompt = f"""
        'あなたは、提供されたコンテキストから情報を抽出する専門家です。 
        文脈に基づいて質問に答えてください。簡潔にして、ハルシネーションを起こさないようにしてください。 
        情報がない場合は、そう言ってください。
        ## コンテキスト
        {prompt_context}
        ## 質問:  
        {user_msg} 
        ## 回答: '
    """
    
    response_df = execute_query("""
        select SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic',?) as response
    """, params=[prompt])

    response = response_df.iat[0,0]

    context_items = set(df_context["ITEM_URL"])
    urls = f"{os.linesep}".join(context_items)

    return f"""{response}

この商品の情報はこちらです。{urls}
"""

# ======= ここから画面 =======

st.title("楽天市場 商品検索")

# チャットログを保存したセッション情報を初期化
if "chat_log" not in st.session_state:
    st.session_state.chat_log = []

user_msg = st.chat_input("ここにメッセージを入力")
if user_msg:
    # 以前のチャットログを表示
    for chat in st.session_state.chat_log:
        with st.chat_message(chat["name"]):
            st.write(chat["msg"])

    # 最新のメッセージを表示
    with st.chat_message(USER_NAME):
        st.write(user_msg)

    # アシスタントのメッセージを表示
    response = generate_response(user_msg)
    with st.chat_message(ASSISTANT_NAME):
        assistant_response_area = st.empty()
        assistant_response_area.write(response)

    # セッションにチャットログを追加
    st.session_state.chat_log.append({"name": USER_NAME, "msg": user_msg})
    st.session_state.chat_log.append({"name": ASSISTANT_NAME, "msg": response})