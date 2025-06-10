
from py2neo import Graph, Node, Relationship
from datetime import datetime
import pandas as pd
import json

# 加载数据
with open('C:/Users/86157/PycharmProjects/数据采集/wx/BERT可视化/cleandata_idea_model.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)
df['CreateTime'] = pd.to_datetime(df['CreateTime'])
df['Date'] = df['CreateTime'].dt.date
df['Hour'] = df['CreateTime'].dt.hour

def time_period(hour):
    if 6 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 18:
        return 'afternoon'
    elif 18 <= hour < 24:
        return 'evening'
    else:
        return 'night'

df['TimePeriod'] = df['Hour'].apply(time_period)

grouped = df.groupby('StrTalker')
results = []

for talker, group in grouped:
    total_msgs = len(group)
    days_active = group['Date'].nunique()
    msg_per_day = total_msgs / days_active
    sentiment_counts = group['Sentiment'].value_counts(normalize=True).to_dict()
    avg_confidence = group['Confidence'].mean()
    period_dist = group['TimePeriod'].value_counts(normalize=True).to_dict()
    last_chat = group['CreateTime'].max()

    results.append({
        'Friend': talker,
        'Messages': total_msgs,
        'DaysActive': days_active,
        'MsgPerDay': round(msg_per_day, 2),
        'SentimentRatio': sentiment_counts,
        'AvgSentimentConfidence': round(avg_confidence, 2),
        'TimePeriodDist': period_dist,
        'LastChat': last_chat
    })

def calc_strength(row):
    score = 0
    score += min(row['MsgPerDay'] / 20, 1) * 0.4
    score += row['SentimentRatio'].get('Positive', 0) * 0.3
    score += row['AvgSentimentConfidence'] * 0.2
    score += 0.1 if (datetime.now() - row['LastChat']).days < 7 else 0
    return round(score, 3)

for r in results:
    r['RelationshipStrength'] = calc_strength(r)

# 连接到 Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "qwer1234"))

# 清除旧图（可选）
graph.run("MATCH (n) DETACH DELETE n")

# 创建主节点
me = Node("Person", name="Me")
graph.create(me)

# 创建好友节点和关系
for r in results:
    friend = Node("Person", name=r['Friend'])
    rel = Relationship(me, "CHATS_WITH", friend,
                       messages=int(r['Messages']),
                       msg_per_day=float(r['MsgPerDay']),
                       sentiment_positive=round(float(r['SentimentRatio'].get('Positive', 0)), 2),
                       avg_confidence=float(r['AvgSentimentConfidence']),
                       strength=float(r['RelationshipStrength']),
                       last_chat=r['LastChat'].strftime('%Y-%m-%d %H:%M:%S'))
    graph.create(friend | rel)
