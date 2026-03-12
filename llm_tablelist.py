import os
import sys
import json
import configparser
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

import aocc_client_vault
import mysql_connect

# ==============================================================================
# 請在這裡指定你要分析的 Project 名稱 (例如 "accaci", "bsgap" 等)
# 留空字串 "" 則代表分析資料庫裡所有的 models
TARGET_PROJECT = "accaci"
# ==============================================================================

def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"[ERROR] 找不到設定檔: {config_path}")
        sys.exit(1)
        
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    return config

def extract_json_from_result(result_text):
    """
    嘗試從 LLM 的回覆中精準取出 JSON (List 或 Dict)。
    """
    text = result_text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
        
    if text.endswith("```"):
        text = text[:-3]
    
    try:
        # 尋找第一個 [ 或 {
        start_idx = -1
        end_idx = -1
        
        for i, char in enumerate(text):
            if char in ['[', '{']:
                start_idx = i
                break
                
        for i in range(len(text)-1, -1, -1):
            if text[i] in [']', '}']:
                end_idx = i
                break
                
        if start_idx != -1 and end_idx != -1:
            json_str = text[start_idx:end_idx+1]
            return json.loads(json_str)
        else:
            return json.loads(text)
    except Exception as e:
        print(f"[WARNING] 無法解析 JSON: {e}")
        print(f"[DEBUG] 原始回覆內容:\n{result_text}")
        return None

def analyze_and_insert(model_id, file_name, file_path, system_prompt, model_version, db_conn):
    print(f"\n[INFO] 正在分析檔案: {file_name} (所屬 model_id: {model_id})")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            code_content = f.read()
    except Exception as e:
        print(f"[ERROR] 無法讀取檔案 {file_path}: {e}")
        return
        
    try:
        result_text = aocc_client_vault.ask_llm(
            model=model_version,
            user_prompt=system_prompt,
            stats_markdown=f"【檔案名稱】: {file_name}\n【程式碼內容】:\n```\n{code_content}\n```"
        )
        
        # 解析 LLM 回傳的 JSON
        data = extract_json_from_result(result_text)
        
        if data is None:
            print(f"[ERROR] 檔案 {file_name} 解析 JSON 失敗，跳過寫入。")
            return
            
        # 確保 data 是一個 List
        if isinstance(data, dict):
            # 有時候 LLM 會包一層例如 {"tables": [...]}
            found_list = False
            for k, v in data.items():
                if isinstance(v, list):
                    data = v
                    found_list = True
                    break
            if not found_list:
                data = [data]
                
        if not isinstance(data, list):
            print(f"[ERROR] 檔案 {file_name} 預期 LLM 回傳 List，但得到 {type(data)}，跳過寫入。")
            return
            
        if len(data) == 0:
            print(f"  - 檔案 {file_name} 似乎沒有產出任何資料表。")
            return
            
        # 準備寫入每張 table 到 tablelist
        for table_info in data:
            table_name = table_info.get("table_name", "").strip()
            upstream_table = table_info.get("upstream_table", "").strip()
            
            if not table_name:
                continue
                
            print(f"  - 找到產出表: {table_name}")
            print(f"    - Upstream: {upstream_table[:50]}...")
            
            # 從資料庫查詢是否已有此 table_name，若無則取最大 table_id + 1
            table_id = None
            try:
                cursor = db_conn.cursor()
                cursor.execute("SELECT table_id FROM dataserviceflow_dm.tablelist WHERE table_name = %s", (table_name,))
                row = cursor.fetchone()
                if row:
                    table_id = row[0]
                else:
                    cursor.execute("SELECT MAX(CAST(table_id AS UNSIGNED)) FROM dataserviceflow_dm.tablelist")
                    max_row = cursor.fetchone()
                    max_id = max_row[0] if max_row and max_row[0] is not None else 0
                    table_id = f"{(max_id + 1):04d}"
                cursor.close()
            except Exception as e:
                print(f"    [ERROR] 查詢 table_id 失敗: {e}")
                continue
                
            dw_ins_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 寫入 MySQL (UPSERT)
            try:
                cursor = db_conn.cursor()
                insert_query = """
                    INSERT INTO dataserviceflow_dm.tablelist 
                    (table_id, table_name, model_id, upstream_table, dw_ins_time)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    model_id=VALUES(model_id),
                    upstream_table=VALUES(upstream_table),
                    dw_ins_time=VALUES(dw_ins_time)
                """
                cursor.execute(insert_query, (
                    table_id, 
                    table_name, 
                    model_id, 
                    upstream_table, 
                    dw_ins_time
                ))
                db_conn.commit()
                cursor.close()
                print(f"    [SUCCESS] 表 {table_name} 已存入資料庫 (分配到的 table_id: {table_id})")
                
            except Exception as db_err:
                print(f"    [ERROR] 資料庫寫入失敗 ({table_name}): {db_err}")
                
    except Exception as e:
        print(f"[ERROR] LLM 分析失敗 ({file_name}): {e}")

def main():
    base_dir = Path(r"D:\Work\OneDrive - ASUS\ASUS Work_CS BU\12_vibe_coding\data_market\CSGP")
    env_path = base_dir / ".env"
    config_path = base_dir / "config.ini"
    
    # 目標資料夾從 config.ini 中讀取
    print(f"[INFO] 載入環境變數: {env_path}")
    load_dotenv(dotenv_path=env_path)
    model_version = os.getenv("AOCC_MODEL_VERSION", "gpt41")
    
    # 讀取設定檔
    config = load_config(config_path)
    target_path = config.get("Settings", "TargetFile", fallback="").strip()
    target_dir = Path(target_path)
    
    # 直接指定你要使用 config.ini 裡的哪一個 Prompt 區塊
    active_prompt_section = "Prompt_TableList"
    
    if not config.has_section(active_prompt_section):
        print(f"[FATAL] config.ini 中找不到指定的 Prompt 區塊: [{active_prompt_section}]")
        sys.exit(1)
        
    system_prompt = config.get(active_prompt_section, "SystemPrompt", fallback="").strip()
    
    # 取得 MySQL 連線
    print("[INFO] 正在連接 MySQL 資料庫...")
    db_conn = mysql_connect.get_mysql_connection()
    if not db_conn:
        print("[FATAL] 無法連接至資料庫，請檢查 Vault 憑證或網路連線。")
        sys.exit(1)
        
    # 去 modellist 資料表裡打撈你要的 models
    models_to_process = []
    try:
        cursor = db_conn.cursor()
        if TARGET_PROJECT:
            print(f"[INFO] 正在從 modellist 尋找專案為 '{TARGET_PROJECT}' 的 model...")
            cursor.execute("SELECT model_id, model_name FROM dataserviceflow_dm.modellist WHERE project = %s", (TARGET_PROJECT,))
        else:
            print(f"[INFO] 正在從 modellist 尋找所有的 model (未指定專案)...")
            cursor.execute("SELECT model_id, model_name FROM dataserviceflow_dm.modellist")
            
        rows = cursor.fetchall()
        for row in rows:
            models_to_process.append({"model_id": row[0], "model_name": row[1]})
        cursor.close()
    except Exception as e:
        print(f"[FATAL] 查詢 modellist 發生錯誤: {e}")
        sys.exit(1)
        
    if not models_to_process:
        print(f"[WARNING] 找不到任何符合條件的 model 資料。")
        sys.exit(0)
    
    print(f"[INFO] 總共找到 {len(models_to_process)} 支 model 準備分析。")
    print(f"[INFO] 使用模型: {model_version}")
    
    # 跑迴圈，逐一分析檔案並寫入 tablelist
    for i, model in enumerate(models_to_process, 1):
        print(f"\n進度: [{i}/{len(models_to_process)}]")
        # 判斷實體檔案是否存在
        file_path = target_dir / model["model_name"]
        if not file_path.exists():
            print(f"[WARNING] 找不到實體檔案，將跳過分析: {file_path}")
            continue
            
        analyze_and_insert(model["model_id"], model["model_name"], file_path, system_prompt, model_version, db_conn)
        
    # 關閉資料庫連線
    if db_conn.is_connected():
        db_conn.close()
        print("\n[INFO] 資料庫連線已關閉。")
        
    print("\n[INFO] 所有檔案分析與資料庫寫入完畢！")

if __name__ == "__main__":
    main()
