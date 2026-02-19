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
    
    # D1 接口单次处理大量 SQL 容易超时，建议保持在 300-400 条
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
                # 打印出错的 SQL 前段方便调试
                print(f"出错 SQL 片段: {combined_sql[:200]}...")
                exit(1)
        except Exception as e:
            print(f"❌ 网络请求异常: {e}")
            exit(1)
    
    print("✨ 数据同步、FTS 索引及触发器构建大功告成！")

def main():
    csv_url = "https://raw.githubusercontent.com/YuleBest/MobileModels-csv/refs/heads/main/models.csv"
    
    print("正在拉取 CSV...")
    try:
        res = requests.get(csv_url)
        res.raise_for_status()
        df = pd.read_csv(BytesIO(res.content))
    except Exception as e:
        print(f"❌ 获取 CSV 失败: {e}")
        return

    sql_commands = []
    
    # --- 1. 重建表结构 ---
    sql_commands.append("DROP TABLE IF EXISTS phone_models;")
    sql_commands.append("DROP TABLE IF EXISTS phone_models_fts;")
    
    # 创建基础存储表
    sql_commands.append("""
    CREATE TABLE phone_models (
        model TEXT, 
        dtype TEXT, 
        brand TEXT, 
        brand_title TEXT, 
        code TEXT, 
        code_alias TEXT, 
        model_name TEXT, 
        ver_name TEXT
    );""")
    
    # --- 2. 创建性能索引 ---
    # 解决 brand 分组统计 22k 读取问题
    sql_commands.append("CREATE INDEX idx_brand ON phone_models(brand);")
    # 解决 dtype 分组统计问题
    sql_commands.append("CREATE INDEX idx_dtype ON phone_models(dtype);")
    
    # --- 3. 创建全文搜索表 (FTS5) ---
    # 修正：增加了 content_rowid='rowid' 以确保索引和主表行号同步
    sql_commands.append("""
    CREATE VIRTUAL TABLE phone_models_fts USING fts5(
        model, 
        code, 
        code_alias, 
        model_name, 
        brand, 
        content='phone_models', 
        content_rowid='rowid'
    );""")

    # --- 4. 创建自动同步触发器 ---
    # 以后只要往 phone_models 插入数据，FTS 表会自动实时同步索引
    sql_commands.append("""
    CREATE TRIGGER phone_models_ai AFTER INSERT ON phone_models BEGIN
      INSERT INTO phone_models_fts(rowid, model, code, code_alias, model_name, brand)
      VALUES (new.rowid, new.model, new.code, new.code_alias, new.model_name, new.brand);
    END;""")

    # --- 5. 生成插入语句 ---
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
    
    # 特别注意：因为我们加了 AFTER INSERT 触发器，所以插入 phone_models 的时候，
    # fts 表已经自动填好了，不再需要手动执行 INSERT INTO phone_models_fts SELECT...

    # 保存 SQL 预览
    with open("update.sql", "w", encoding="utf-8") as f:
        f.write("\n".join(sql_commands))
    print(f"✅ SQL 已保存到 update.sql，共 {len(sql_commands)} 条指令")

    # --- 6. 执行上传 ---
    if API_TOKEN and ACCOUNT_ID and DATABASE_ID:
        upload_to_d1(sql_commands)
        
        # 保存更新时间 (UTC+8)
        tz = timezone(timedelta(hours=8))
        current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        with open("update_time.txt", "w", encoding="utf-8") as f:
            f.write(current_time)
        print(f"✅ 更新时间已保存: {current_time}")
    else:
        print("⚠️ 缺少环境变量，仅生成 SQL 文件，未执行上传。")

if __name__ == "__main__":
    main()
