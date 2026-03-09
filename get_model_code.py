import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import quote
from dotenv import load_dotenv

def build_auth_url(repo_url, username, password):
    """
    將使用者名稱與密碼加入到 Git 網址中，以利自動 clone。
    同時會對帳密進行 URL 編碼避免特殊字元造成錯誤。
    """
    if not username or not password:
        return repo_url
    
    if "://" not in repo_url:
        return repo_url
        
    protocol, rest = repo_url.split("://", 1)
    u = quote(username.strip(), safe="")
    p = quote(password.strip(), safe="")
    return f"{protocol}://{u}:{p}@{rest}"

def clone_or_pull_repo(auth_url, local_path):
    """
    如果本地沒有該 Git Repo，則執行 clone；如果已存在，則執行 pull 確保最新。
    """
    if local_path.is_dir() and (local_path / ".git").is_dir():
        print(f"[INFO] 儲存庫已存在，正在執行 git pull 更新: {local_path}")
        try:
            subprocess.run(["git", "-C", str(local_path), "pull"], check=True, capture_output=True, text=True)
            print("[INFO] git pull 更新成功。")
            return
        except subprocess.CalledProcessError as e:
            print(f"[WARN] git pull 失敗，將重新 clone。錯誤訊息: {e.stderr}")
            shutil.rmtree(local_path, ignore_errors=True)
            
    local_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] 執行 git clone...")
    try:
        # 使用 auth_url 可能會在 stderr 印出網址（若帶密碼則略有風險），
        # 在開發機上自己使用較無大礙，但還是設定 capture_output=True
        subprocess.run(["git", "clone", auth_url, str(local_path)], check=True, capture_output=True, text=True)
        print("[INFO] git clone 成功。")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] git clone 失敗: {e.stderr}")
        raise

def copy_target_files(src_folder: Path, dest_folder: Path):
    """
    搜尋來源目錄下的所有 .sql 與 .py 檔案，複製到指定的目標目錄中。
    """
    if not src_folder.exists() or not src_folder.is_dir():
        print(f"[ERROR] 來源資料夾 (gp) 不存在，請確認 Git 專案內是否有此目錄: {src_folder}")
        return

    dest_folder.mkdir(parents=True, exist_ok=True)
    count = 0
    
    # 搜尋 gp 資料夾內所有的檔案 (包含子資料夾內)
    for file_path in src_folder.rglob("*"):
        if file_path.is_file() and file_path.suffix.lower() in [".sql", ".py"]:
            # 將檔案複製到 model 目錄下
            dest_file = dest_folder / file_path.name
            
            # 使用 copy2 可以保留檔案的 metadata (如修改時間)
            shutil.copy2(file_path, dest_file)
            print(f"[INFO] 複製檔案: {file_path.name}")
            count += 1
            
    print(f"[DONE] 任務完成，共複製了 {count} 個檔案至 {dest_folder}")

def main():
    # 1. 定義路徑與環境
    base_dir = Path(r"D:\Work\OneDrive - ASUS\ASUS Work_CS BU\12_vibe_coding\data_market\CSGP")
    env_path = base_dir / ".env"
    
    # 檔案最終要存放的位置
    dest_folder = base_dir / "model"
    
    # 2. 讀取 .env 中的 Git 帳密與網址清單
    print(f"[INFO] 載入環境變數: {env_path}")
    load_dotenv(dotenv_path=env_path)
    
    username = os.getenv("GIT_USERNAME", "")
    password = os.getenv("GIT_PASSWORD", "")
    repos_json_str = os.getenv("GIT_REPOS_JSON", "")
    
    if not username or not password:
        print("[WARN] 無法從 .env 讀取到 GIT_USERNAME 或 GIT_PASSWORD，將嘗試不帶帳密下載。")
        
    if not repos_json_str:
        print("[FATAL] 無法從 .env 讀取到 GIT_REPOS_JSON，程式中止。")
        return
        
    import json
    try:
        repos_list = json.loads(repos_json_str)
    except Exception as e:
        print(f"[FATAL] 解析 GIT_REPOS_JSON 失敗，請確認 .env 內的格式是否正確: {e}")
        return
        
    print(f"[INFO] 偵測到 {len(repos_list)} 個 Git 儲存庫設定，準備開始處理。")
    
    # 3. 逐一遍歷所有設定好的儲存庫
    for repo in repos_list:
        repo_id = repo.get("id", "unknown_repo")
        repo_url = repo.get("url", "")
        
        if not repo_url:
            print(f"[WARN] 儲存庫 {repo_id} 沒有 url，略過。")
            continue
            
        print(f"\n======================================")
        print(f"[INFO] 開始處理專案: {repo_id} ({repo_url})")
        
        # Git 從線上載下來後暫存的位置 (避免弄髒工作區，放在一個自建的 cache 資料夾內，並以 ID 分離)
        git_cache_dir = base_dir / "temp_git_cache" / repo_id
        gp_folder = git_cache_dir / "gp"
        
        # 組合認證網址並取得檔案
        auth_url = build_auth_url(repo_url, username, password)
        
        try:
            # 下載或是更新專案
            clone_or_pull_repo(auth_url, git_cache_dir)
            
            # 篩選並複製對應的 .py 與 .sql 檔 (複製到共同的 destination)
            copy_target_files(gp_folder, dest_folder)
            
        except Exception as e:
            print(f"[ERROR] 處理專案 {repo_id} 時發生未預期錯誤: {e}")

if __name__ == "__main__":
    main()
