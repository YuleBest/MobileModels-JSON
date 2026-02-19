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
    
    # 稍微调小 batch_size 确保复杂索引创建不超时
    batch_size = 400 
    for i in range(0, len(sql_statements), batch_size):
        batch = sql_statements[i : i + batch_size]
        combined_sql = "\n".join(batch)
        
        print(f"正在上传第 {i} 到 {i + len(batch)} 行...")
        try:
            response = requests.post(url, headers=headers, json={"sql": combined_sql})
            result = response.json()
            if not result.get("success"):
                print(f"❌ 上传失败！错误信息: {result.get('errors')}")
                print(f"出错 SQL 片段: {combined_sql[:200]}...")
                exit(1)
        except Exception as e:
            print(f"❌ 网络请求异常: {e}")
            exit(1)
    
    print("✨ 数据同步与索引优化大功告成！")

def main():
    csv_url = "https://raw.githubusercontent.com/YuleBest/MobileModels-csv/refs/heads/main/models.csv"
    
    print("正在拉取 CSV...")
    res = requests.get(csv_url)
    df = pd.read_csv(BytesIO(res.content))

    sql_commands = []
    
    # --- 1. 初始化表结构与索引 ---
    sql_commands.append("DROP TABLE IF EXISTS phone_models;")
    sql_commands.append("DROP TABLE IF EXISTS phone_models_fts;") # 删掉旧的全文索引表
    
    # 普通存储表
    sql_commands.append("CREATE TABLE phone_models (model TEXT, dtype TEXT, brand TEXT, brand_title TEXT, code TEXT, code_alias TEXT, model_name TEXT, ver_name TEXT);")
    
    # 针对第一个查询：给 brand 加索引，解决 22k 读取问题
    sql_commands.append("CREATE INDEX idx_brand ON phone_models(brand);")
    
    # 针对第二个查询：创建全文搜索虚表 (FTS5)
    # 我们只对需要模糊搜索的字段建索引，节省空间
    sql_commands.append("CREATE VIRTUAL TABLE phone_models_fts USING fts5(model, code, code_alias, model_name, brand, content='phone_models');")

    # --- 2. 生成插入语句 ---
    for _, row in df.iterrows():
        clean_values = []
        for v in row:
            if pd.isnull(v):
                clean_values.append("NULL")
            else:
                safe_val = str(v).replace("'", "''")
                clean_values.append(f"'{safe_val}'")
        
        # 插入基础表
        sql_commands.append(f"INSERT INTO phone_models VALUES ({', '.join(clean_values)});")
    
    # --- 3. 同步数据到全文索引表 ---
    # 这一步能让 FTS 表填入数据
    sql_commands.append("INSERT INTO phone_models_fts(rowid, model, code, code_alias, model_name, brand) SELECT rowid, model, code, code_alias, model_name, brand FROM phone_models;")

    # 保存 SQL 预览
    with open("update.sql", "w", encoding="utf-8") as f:
        f.write("\n".join(sql_commands))
    print("✅ 优化后的 SQL 已保存到 update.sql")

    # --- 4. 开始上传 ---
    if API_TOKEN and ACCOUNT_ID and DATABASE_ID:
        upload_to_d1(sql_commands)
        
        tz = timezone(timedelta(hours=8))
        current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        with open("update_time.txt", "w", encoding="utf-8") as f:
            f.write(current_time)
        print(f"✅ 更新时间已保存: {current_time}")
    else:
        print("❌ 缺少环境变量")
        exit(1)

if __name__ == "__main__":
    main()
