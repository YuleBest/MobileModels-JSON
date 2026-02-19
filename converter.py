import pandas as pd
import requests
from io import BytesIO
import os
import hashlib
from datetime import datetime, timedelta, timezone

# 配置信息
API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
DATABASE_ID = os.getenv("CLOUDFLARE_DATABASE_ID")
CSV_URL = "https://raw.githubusercontent.com/YuleBest/MobileModels-csv/refs/heads/main/models.csv"
MD5_FILE = "last_csv_md5.txt"

def get_file_md5(content):
    return hashlib.md5(content).hexdigest()

def upload_to_d1(sql_statements):
    url = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/d1/database/{DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
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
                exit(1)
        except Exception as e:
            print(f"❌ 网络请求异常: {e}")
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

    # --- 核心：MD5 校验逻辑 ---
    new_md5 = get_file_md5(new_content)
    
    if os.path.exists(MD5_FILE):
        with open(MD5_FILE, "r") as f:
            old_md5 = f.read().strip()
        
        if new_md5 == old_md5:
            print(f"✅ MD5 匹配 ({new_md5})，数据未变动。跳过 D1 更新，省下 11k 写入额度！")
            return # 数据没变，直接收工
    
    print(f"🚀 数据已变动 (New MD5: {new_md5})，开始生成 SQL...")

    # --- 流程继续：生成 SQL ---
    df = pd.read_csv(BytesIO(new_content))
    sql_commands = []
    
    # 依然采用全量重刷逻辑（因为最稳，且现在有了 MD5 拦截，不怕浪费额度了）
    sql_commands.append("DROP TABLE IF EXISTS phone_models;")
    sql_commands.append("DROP TABLE IF EXISTS phone_models_fts;")
    sql_commands.append("CREATE TABLE phone_models (model TEXT, dtype TEXT, brand TEXT, brand_title TEXT, code TEXT, code_alias TEXT, model_name TEXT, ver_name TEXT);")
    sql_commands.append("CREATE INDEX idx_brand ON phone_models(brand);")
    sql_commands.append("CREATE INDEX idx_dtype ON phone_models(dtype);")
    
    # FTS5 表 (增加 content_rowid 提高效率)
    sql_commands.append("CREATE VIRTUAL TABLE phone_models_fts USING fts5(model, code, code_alias, model_name, brand, content='phone_models', content_rowid='rowid');")

    # 创建同步触发器 (这样插入基础表时，FTS 自动更新)
    sql_commands.append("""
    CREATE TRIGGER phone_models_ai AFTER INSERT ON phone_models BEGIN
      INSERT INTO phone_models_fts(rowid, model, code, code_alias, model_name, brand)
      VALUES (new.rowid, new.model, new.code, new.code_alias, new.model_name, new.brand);
    END;""")

    for _, row in df.iterrows():
        clean_values = []
        for v in row:
            if pd.isnull(v):
                clean_values.append("NULL")
            else:
                # 转义单引号防 SQL 注入
                safe_val = str(v).replace("'", "''")
                clean_values.append(f"'{safe_val}'")
        sql_commands.append(f"INSERT INTO phone_models VALUES ({', '.join(clean_values)});")

    # --- 开始上传 ---
    if API_TOKEN and ACCOUNT_ID and DATABASE_ID:
        upload_to_d1(sql_commands)
        
        # 写入成功后，更新本地 MD5 文件
        with open(MD5_FILE, "w") as f:
            f.write(new_md5)
        
        # 记录更新时间
        tz = timezone(timedelta(hours=8))
        current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        with open("update_time.txt", "w", encoding="utf-8") as f:
            f.write(current_time)
        print(f"✨ 数据同步完成！MD5 已更新。更新时间: {current_time}")
    else:
        print("❌ 缺少环境变量，仅生成了本地文件。")

if __name__ == "__main__":
    main()
