import os
import sys
import configparser
from pathlib import Path
from dotenv import load_dotenv

# 引用我們剛剛重新命名的基礎版 aocc 模組
import aocc_client_basic

def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"[ERROR] 找不到設定檔: {config_path}")
        sys.exit(1)
        
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    return config

def main():
    # 1. 讀取路徑與設定
    base_dir = Path(r"D:\Work\OneDrive - ASUS\ASUS Work_CS BU\12_vibe_coding\data_market\CSGP")
    env_path = base_dir / ".env"
    config_path = base_dir / "config.ini"
    
    print(f"[INFO] 載入環境變數: {env_path}")
    # ★ 關鍵：在呼叫 aocc_client_basic 之前，我們必須先把 .env 載入到系統變數裡
    load_dotenv(dotenv_path=env_path)
    
    # 2. 讀取 config.ini (包含要分析哪支檔案，以及分析的特殊指令)
    config = load_config(config_path)
    target_file = config.get("Settings", "TargetFile", fallback="")
    system_prompt = config.get("Prompt", "SystemPrompt", fallback="")
    
    if not target_file or not os.path.exists(target_file):
        print(f"[FATAL] 在 config.ini 中設定的 TargetFile 無效或不存在: {target_file}")
        sys.exit(1)
        
    print(f"[INFO] 準備分析的目標檔案: {target_file}")
    
    # 3. 讀取目標程式碼
    try:
        with open(target_file, "r", encoding="utf-8") as f:
            code_content = f.read()
    except Exception as e:
        print(f"[FATAL] 無法讀取目標檔案: {e}")
        sys.exit(1)
        
    print("[INFO] 正在請求 LLM 模型分析程式碼，此過程透過 aocc_client_basic 進行連線，請稍候...")
    
    # 從 .env 中讀取我們想要使用的模型
    model_version = os.getenv("AOCC_MODEL_VERSION", "gpt41")
    
    # 4. 呼叫 aocc_client_basic 進行分析
    try:
        result = aocc_client_basic.ask_llm(
            model=model_version,
            user_prompt=system_prompt,  # 分析指令
            stats_markdown=f"```python\n{code_content}\n```" # 要分析的程式碼
        )
        
        # 5. 印出結果
        print("\n" + "="*50)
        print("✨ LLM 分析結果 ✨")
        print("="*50)
        print(result)
        print("="*50)
    except Exception as e:
        print(f"[FATAL] 透過 aocc_client_basic 呼叫 LLM 失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
