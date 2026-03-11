import logging
import mysql.connector
import mysql_connect

# ==============================================================================
# 請在這裡指定你想要建立的資料庫名稱
# ==============================================================================
TARGET_DB_NAME = "dataserviceflow_dm"

# 啟用 Logger
logger = logging.getLogger("mysql_db_setup")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
if not logger.handlers:
    logger.addHandler(console_handler)

def main():
    logger.info("開始執行 MySQL 資料庫設定 (建立 dataserviceflow_dm)...")
    
    # 透過 mysql_connect 模組取得連線
    conn = mysql_connect.get_mysql_connection()
    
    if not conn:
        logger.error("無法取得資料庫連線，程式結束。")
        return

    try:
        cursor = conn.cursor()
        
        # 建立資料庫的 SQL 語法 (IF NOT EXISTS 可以避免資料庫已存在時報錯)
        # 讀取最上方設定的 TARGET_DB_NAME 變數
        db_name = TARGET_DB_NAME
        create_db_query = f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        
        logger.info(f"執行 SQL: {create_db_query}")
        cursor.execute(create_db_query)
        
        logger.info(f"成功確認/建立資料庫: '{db_name}'")
        
        cursor.close()
        
    except mysql.connector.Error as err:
        logger.error(f"MySQL 執行錯誤: {err}")
    except Exception as e:
        logger.error(f"發生未知的錯誤: {e}")
    finally:
        if conn.is_connected():
            conn.close()
            logger.info("MySQL 連線已關閉。")

if __name__ == "__main__":
    main()
