import pandas as pd
import requests
from io import BytesIO
import os
from datetime import datetime, timedelta, timezone

# 从环境变量获取配置
API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
DATABASE_ID = os.getenv("CLOUDFLARE_DATABASE_ID")

def upload_to_d1(sql_statements):
    url = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/d1/database/{DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # 分批上传，每组 500 条
    batch_size = 500
    for i in range(0, len(sql_statements), batch_size):
        batch = sql_statements[i : i + batch_size]
        combined_sql = "\n".join(batch)
        
        print(f"正在上传第 {i} 到 {i + len(batch)} 行...")
        try:
            response = requests.post(url, headers=headers, json={"sql": combined_sql})
            result = response.json()
            if not result.get("success"):
                print(f"❌ 上传失败！错误信息: {result.get('errors')}")
                # 打印出具体的 SQL 语句方便排查，但只打印前 100 个字符
                print(f"出错 SQL 片段: {combined_sql[:100]}...")
                exit(1)
        except Exception as e:
            print(f"❌ 网络请求异常: {e}")
            exit(1)
    
    print("✨ 数据同步大功告成！")

def main():
    csv_url = "https://raw.githubusercontent.com/YuleBest/MobileModels-csv/refs/heads/main/models.csv"
    
    print("正在拉取 CSV...")
    res = requests.get(csv_url)
    df = pd.read_csv(BytesIO(res.content))

    sql_commands = []
    # 1. 重建表结构
    sql_commands.append("DROP TABLE IF EXISTS phone_models;")
    sql_commands.append("CREATE TABLE phone_models (model TEXT, dtype TEXT, brand TEXT, brand_title TEXT, code TEXT, code_alias TEXT, model_name TEXT, ver_name TEXT);")
    
    # 2. 生成插入语句
    for _, row in df.iterrows():
        clean_values = []
        for v in row:
            if pd.isnull(v):
                clean_values.append("NULL")
            else:
                # 重点：这里用最稳妥的办法转义单引号
                safe_val = str(v).replace("'", "''")
                clean_values.append(f"'{safe_val}'")
        
        sql_commands.append(f"INSERT INTO phone_models VALUES ({', '.join(clean_values)});")
    
    # 2.5 保存 SQL 到文件
    with open("update.sql", "w", encoding="utf-8") as f:
        f.write("\n".join(sql_commands))
    print("✅ SQL 已保存到 update.sql")

    # 3. 开始上传
    if API_TOKEN and ACCOUNT_ID and DATABASE_ID:
        upload_to_d1(sql_commands)
        
        # 4. 保存当前时间 (UTC+8)
        tz = timezone(timedelta(hours=8))
        current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        with open("update_time.txt", "w", encoding="utf-8") as f:
            f.write(current_time)
        print(f"✅ 更新时间已保存: {current_time}")
    else:
        print("❌ 缺少环境变量：API_TOKEN, ACCOUNT_ID 或 DATABASE_ID")
        exit(1)

if __name__ == "__main__":
    main()
