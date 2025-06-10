# %% 准备训练数据
#对的，这个是第一步
# 数据库和表配置
DB_NAME = "MSG7.db"
TABLE_NAME = "MSG"

# 连接数据库并获取所有聊天记录
with sqlite3.connect(DB_NAME) as conn:
    df = pd.read_sql_query(
        f"SELECT StrTalker, CreateTime, StrContent FROM {TABLE_NAME} "
        f"WHERE Type = 1 AND IsSender = 0 AND StrTalker NOT LIKE '%@chatroom'",
        conn
    )

# 按 StrTalker 和 CreateTime 排序
df = df.sort_values(by=["StrTalker", "CreateTime"])

# 初始化合并历史记录
merged_history = []
if not df.empty:
    current_group = [df.iloc[0]["StrContent"]]
    group_start_time = df.iloc[0]["CreateTime"]
    current_talker = df.iloc[0]["StrTalker"]
    current_entry = df.iloc[0].to_dict()

    # 遍历消息进行分组
    for index, row in df.iloc[1:].iterrows():
        current_time = row["CreateTime"]
        content = row["StrContent"]
        talker = row["StrTalker"]

        # 检查是否同一 StrTalker 且时间差小于5分钟（300秒）
        if talker == current_talker and current_time - group_start_time < 300:
            current_group.append(content)
            # 更新当前组的内容和时间
            current_entry["StrContent"] = "\n".join(current_group)
            current_entry["CreateTime"] = current_time
        else:
            # 保存当前组并开始新组
            merged_history.append(current_entry)
            current_entry = row.to_dict()
            current_group = [content]
            group_start_time = current_time
            current_talker = talker

    # 添加最后一组
    merged_history.append(current_entry)

# 转换为 DataFrame
train_df = pd.DataFrame(merged_history)
train_df["CreateTime"] = pd.to_datetime(train_df["CreateTime"], unit="s").dt.strftime("%Y-%m-%d %H:%M:%S")
# 保存到 JSON
train_df.to_json("train_allperson.json", orient="records", force_ascii=False, indent=4)