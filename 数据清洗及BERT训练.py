import pandas as pd
from transformers import pipeline
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import json
import re
#这个是对的第二步
# 读取数据
with open('train_allperson.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 转换为DataFrame
df = pd.DataFrame(data)

# 清洗空内容和只包含表情的内容
df = df[df['StrContent'].str.strip().astype(bool)]

# 标准化时间格式
df['CreateTime'] = pd.to_datetime(df['CreateTime'])

# 可选：去掉非中文字符、URL、特殊符号等（简单示例）
import re
def clean_text(text):
    text = re.sub(r'[^\u4e00-\u9fa5。！？\n]', '', text)  # 保留中文及常见符号
    return text

df['CleanContent'] = df['StrContent'].apply(clean_text)
df=df.drop(columns=['StrContent'])

# classifier = pipeline("text-classification", model="uer/roberta-base-finetuned-jd-binary-chinese", tokenizer="uer/roberta-base-finetuned-jd-binary-chinese")
classifier = pipeline("text-classification",
                      model="IDEA-CCNL/Erlangshen-RoBERTa-110M-Sentiment",
                      tokenizer="IDEA-CCNL/Erlangshen-RoBERTa-110M-Sentiment")
def predict_emotion(text):
    try:
        result = classifier(text[:128])[0]  # 截断避免超过max_length
        return result['label'], float(result['score'])
    except:
        return "neutral", 0.5  # 出错时设为中性

df[['Sentiment', 'Confidence']] = df['CleanContent'].apply(lambda x: pd.Series(predict_emotion(x)))
# df=df.drop(columns=['StrContent'])
# df["CreateTime"] = pd.to_datetime(df["CreateTime"], unit="ms").dt.strftime("%Y-%m-%d %H:%M:%S")
df.to_json("cleandata_idea_model.json", orient="records", force_ascii=False, indent=4)
