import os
import logging
import mysql.connector
from dotenv import load_dotenv
from npspo_vault_client import VaultClient

# 啟用 Logger
logger = logging.getLogger("mysql_connect")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
if not logger.handlers:
    logger.addHandler(console_handler)

# 載入 .env 檔案
load_dotenv()

def get_mysql_connection():
    """
    透過 Vault 取得密碼，並回傳 MySQL 連線物件。
    如果失敗則回傳 None。
    """
    # 0. 初始化 Vault Client
    vault_client = VaultClient(logger=logger)

    # 1. 開發模式中使用 User Token 登入 Vault
    try:
        user_token = vault_client.get_user_token()
        logger.info("成功使用 User Token 登入 Vault 伺服器")
    except Exception as e:
        logger.error(f"使用 User Token 登入 Vault 失敗: {e}")
        return None

    # 2. 取得 MySQL 帳號密碼
    try:
        # 從 .env 讀取 Vault 金鑰存取的路徑
        secret_path = os.getenv("secret_path_apza005npd") 
        if not secret_path:
            logger.error("未在 .env 中設定 secret_path_apza005npd")
            return None

        # 使用 user_token 取得 secret
        secret_data = vault_client.get_vault_secret(secret_path=secret_path, token=user_token)    
        
        # 嘗試處理不同大小寫的 key，確保能順利抓到帳密
        username = secret_data.get("Username") or secret_data.get("username") or ""
        password = secret_data.get("Password") or secret_data.get("password") or ""
        
        if not username or not password:
            logger.error("無法從 Vault 取得有效的 MySQL 帳號或密碼")
            return None
            
        logger.info("成功從 Vault 取得 MySQL 登入憑證")
    except Exception as e:
        logger.error(f"取得 MySQL 憑證失敗: {e}")
        return None

    # 3. 準備 MySQL 連線資訊 (從 .env 讀取，若無則報錯或使用預設值)
    # 取優先順序: MYSQL_HOST_109 -> MYSQL_HOST
    host = os.getenv("MYSQL_HOST_109") or os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT_109") or os.getenv("MYSQL_PORT") or "3306"

    if not host:
        logger.error("未在 .env 中設定 MYSQL_HOST_109 或 MYSQL_HOST，無法連線資料庫")
        return None

    # 4. 建立連線並回傳
    try:
        logger.info(f"正在連線至 MySQL 伺服器 ({host}:{port}) 作為 '{username}'...")
        
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=username,
            password=password
        )
        
        if conn.is_connected():
            logger.info("成功連線至 MySQL 伺服器！")
            return conn
            
    except mysql.connector.Error as err:
        logger.error(f"MySQL 連線錯誤: {err}")
    except Exception as e:
        logger.error(f"發生未知的錯誤: {e}")

    return None

if __name__ == "__main__":
    conn = get_mysql_connection()
    if conn:
        logger.info("測試連線成功，即將關閉連線。")
        conn.close()
    else:
        logger.error("測試連線失敗。")
