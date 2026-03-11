import logging
import mysql.connector
import mysql_connect

# ==============================================================================
# 【危險操作提醒】請在這裡指定你確定要「永久刪除」的資料庫名稱
# 警告：一旦執行成功，該資料庫內的所有資料表和資料都會被清空且無法復原！
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
    logger.info("開始執行 MySQL 資料庫設定 (刪除 dataserviceflow_dm)...")
    
    # 透過 mysql_connect 模組取得連線
    conn = mysql_connect.get_mysql_connection()
    
    if not conn:
        logger.error("無法取得資料庫連線，程式結束。")
        return

    try:
        cursor = conn.cursor()
        
        # 刪除資料庫的 SQL 語法
        # 讀取最上方設定的 TARGET_DB_NAME 變數
        db_name = TARGET_DB_NAME 
        drop_db_query = f"DROP DATABASE IF EXISTS {db_name};"
        
        logger.warning(f"即將執行高風險 SQL: {drop_db_query}")
        cursor.execute(drop_db_query)
        
        logger.info(f"成功確認/刪除資料庫: '{db_name}'")
        
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
