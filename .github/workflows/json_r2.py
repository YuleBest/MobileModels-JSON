import pandas as pd
import requests
from io import BytesIO
import os
import hashlib
import json
import boto3  # 需要 pip install boto3
from datetime import datetime, timedelta, timezone

# 配置信息
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT = os.getenv("R2_ENDPOINT_URL")

CSV_URL = "https://raw.githubusercontent.com/YuleBest/MobileModels-csv/refs/heads/main/models.csv"
MD5_FILE = "last_csv_md5.txt"
JSON_FILENAME = "models.json"

def get_file_md5(content):
    return hashlib.md5(content).hexdigest()

def upload_to_r2(json_data):
    print("🚀 准备上传至 R2...")
    s3 = boto3.client(
        service_name='s3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto" # R2 固定填 auto
    )
    
    try:
        s3.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=JSON_FILENAME,
            Body=json_data,
            ContentType='application/json',
            CacheControl='public, max-age=3600' # 浏览器缓存1小时
        )
        print("✅ R2 上传成功！")
    except Exception as e:
        print(f"❌ R2 上传失败: {e}")
        exit(1)

def main():
    print("正在拉取远程 CSV...")
    try:
        res = requests.get(CSV_URL)
        res.raise_for_status()
        new_content = res.content
    except Exception as e:
        print(f"❌ 拉取失败: {e}")
        return

    new_md5 = get_file_md5(new_content)
    
    if os.path.exists(MD5_FILE):
        with open(MD5_FILE, "r") as f:
            old_md5 = f.read().strip()
        if new_md5 == old_md5:
            print(f"✅ MD5 匹配，数据未变动。跳过 R2 更新。")
            return
    
    print(f"🚀 数据变动，开始处理 JSON...")

    # 使用 Pandas 处理数据
    df = pd.read_csv(BytesIO(new_content))
    
    # 清洗：将空值转为 null，确保 JSON 格式正确
    # to_dict('records') 直接生成前端最喜欢的 [{...}, {...}] 格式
    json_list = df.where(pd.notnull(df), None).to_dict(orient='records')
    
    # 转换为 JSON 字符串，去掉空格压缩体积
    json_data = json.dumps(json_list, ensure_ascii=False, separators=(',', ':'))

    # 执行 R2 上传
    if R2_ACCESS_KEY and R2_SECRET_KEY:
        upload_to_r2(json_data)
        
        with open(MD5_FILE, "w") as f:
            f.write(new_md5)
        
        tz = timezone(timedelta(hours=8))
        current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        print(f"✨ 同步完成！更新时间: {current_time}")
    else:
        print("❌ 缺少 R2 环境变量。")

if __name__ == "__main__":
    main()
