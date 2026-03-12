import os
import sys
import json
import configparser
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

import aocc_client_vault
import mysql_connect

def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"[ERROR] 找不到設定檔: {config_path}")
        sys.exit(1)
        
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    return config

def extract_json_from_result(result_text):
    """
    嘗試從 LLM 的回覆中精準取出 JSON 字串。
    如果你使用的模型喜歡講廢話，這段可以幫忙過濾掉前後文。
    """
    text = result_text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.endswith("```"):
        text = text[:-3]
    
    try:
        # 尋找第一個 { 和最後一個 }
        start_idx = text.find("{")
        end_idx = text.rfind("}")
        if start_idx != -1 and end_idx != -1:
            json_str = text[start_idx:end_idx+1]
            return json.loads(json_str)
        else:
            return json.loads(text)
    except Exception as e:
        print(f"[WARNING] 無法解析 JSON: {e}")
        print(f"[DEBUG] 原始回覆內容:\n{result_text}")
        return None

def analyze_and_insert(file_path, system_prompt, model_version, db_conn):
    """將單一檔案丟給 LLM 分析，並將結果存入資料庫"""
    file_name = file_path.name
    
    # 從資料庫查詢是否已有此檔案，若無則取最大號碼 + 1
    model_id = None
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT model_id FROM dataserviceflow_dm.modellist WHERE model_name = %s", (file_name,))
        row = cursor.fetchone()
        if row:
            model_id = row[0]
        else:
            cursor.execute("SELECT MAX(CAST(model_id AS UNSIGNED)) FROM dataserviceflow_dm.modellist")
            max_row = cursor.fetchone()
            max_id = max_row[0] if max_row and max_row[0] is not None else 0
            model_id = f"{(max_id + 1):04d}"
        cursor.close()
    except Exception as e:
        print(f"[ERROR] 查詢 model_id 失敗: {e}")
        return
    
    print(f"\n[INFO] 正在分析檔案: {file_name} (分配到的 model_id: {model_id})")
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
        
        if not data:
            print(f"[ERROR] 檔案 {file_name} 解析失敗，跳過寫入。")
            return
            
        model_summary = data.get("model_summary", "")
        upstream_model = data.get("upstream_model", "")
        project = data.get("project", "x")  # 從 LLM 回傳的結果中取出 project，預設為 x
        dw_ins_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"  - Project: {project}")
        print(f"  - Summary: {model_summary[:50]}...")
        print(f"  - Upstream: {upstream_model[:50]}...")
        
        # 寫入 MySQL 邏輯
        try:
            cursor = db_conn.cursor()
            
            # 使用 REPLACE INTO 或是 INSERT INTO ... ON DUPLICATE KEY UPDATE 避免重複寫入報錯
            insert_query = """
                INSERT INTO dataserviceflow_dm.modellist 
                (model_id, model_name, project, model_summary, upstream_model, dw_ins_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                project=VALUES(project),
                model_summary=VALUES(model_summary),
                upstream_model=VALUES(upstream_model),
                dw_ins_time=VALUES(dw_ins_time)
            """
            cursor.execute(insert_query, (
                model_id, 
                file_name, 
                project, 
                model_summary, 
                upstream_model, 
                dw_ins_time
            ))
            db_conn.commit()
            cursor.close()
            print(f"[SUCCESS] 檔案 {file_name} 分析結果已存入資料庫。")
            
        except Exception as db_err:
            print(f"[ERROR] 資料庫寫入失敗 ({file_name}): {db_err}")
            
    except Exception as e:
        print(f"[ERROR] LLM 分析失敗 ({file_name}): {e}")

def main():
    base_dir = Path(r"D:\Work\OneDrive - ASUS\ASUS Work_CS BU\12_vibe_coding\data_market\CSGP")
    env_path = base_dir / ".env"
    config_path = base_dir / "config.ini"
    
    # 1. 載入環境變數
    print(f"[INFO] 載入環境變數: {env_path}")
    load_dotenv(dotenv_path=env_path)
    model_version = os.getenv("AOCC_MODEL_VERSION", "gpt41")
    
    # 2. 讀取設定檔
    config = load_config(config_path)
    target_path = config.get("Settings", "TargetFile", fallback="").strip()
    
    # 這裡直接指定你要使用 config.ini 裡的哪一個 Prompt 區塊
    active_prompt_section = "Prompt_ModelList"
    
    if not config.has_section(active_prompt_section):
        print(f"[FATAL] config.ini 中找不到指定的 Prompt 區塊: [{active_prompt_section}]")
        sys.exit(1)
        
    system_prompt = config.get(active_prompt_section, "SystemPrompt", fallback="").strip()
    
    if not target_path or not os.path.exists(target_path):
        print(f"[FATAL] config.ini 中的 TargetFile 路徑無效或不存在: {target_path}")
        sys.exit(1)
        
    # 3. 取得 MySQL 連線
    print("[INFO] 正在連接 MySQL 資料庫...")
    db_conn = mysql_connect.get_mysql_connection()
    if not db_conn:
        print("[FATAL] 無法連接至資料庫，請檢查 Vault 憑證或網路連線。")
        sys.exit(1)
        
    # 4. 判斷傳入的是檔案還是資料夾
    target_path_obj = Path(target_path)
    files_to_process = []

    if target_path_obj.is_file():
        files_to_process.append(target_path_obj)
    elif target_path_obj.is_dir():
        print(f"[INFO] 準備搜尋 {target_path} 底下的檔案...")
        valid_extensions = {".py", ".sql"} 
        for root, _, files in os.walk(target_path_obj):
            for file in files:
                if Path(file).suffix.lower() in valid_extensions:
                    files_to_process.append(Path(root) / file)
                    
        if not files_to_process:
            print(f"[WARNING] 找不到支援的檔案格式 ({valid_extensions})")
            sys.exit(0)
    
    print(f"[INFO] 總共找到 {len(files_to_process)} 支檔案準備分析。")
    print(f"[INFO] 使用模型: {model_version}")
    
    # 5. 跑迴圈，逐一分析檔案並寫入 DB
    for i, file_path in enumerate(files_to_process, 1):
        print(f"\n進度: [{i}/{len(files_to_process)}]")
        analyze_and_insert(file_path, system_prompt, model_version, db_conn)
        
    # 關閉資料庫連線
    if db_conn.is_connected():
        db_conn.close()
        print("\n[INFO] 資料庫連線已關閉。")
        
    print("\n[INFO] 所有檔案分析與資料庫寫入完畢！")

if __name__ == "__main__":
    main()
