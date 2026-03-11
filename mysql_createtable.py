import logging
import mysql.connector
import mysql_connect

# ==============================================================================
# 請在這裡指定你想要建立資料表的 目標資料庫、資料表名稱、以及 欄位定義
# ==============================================================================

# 1. 這是你要把資料表建在哪一個資料庫底下 (例如剛剛建好的 dataserviceflow_dm)
TARGET_DB_NAME = "dataserviceflow_dm"

# 2. 這是你要建立的資料表名稱
TARGET_TABLE_NAME = "columnlist"

# 3. 這是資料表的欄位定義 (欄位名稱 加上 資料型態，用逗號隔開)
# 常見型態參考：
# INT: 整數 / VARCHAR(255): 長度255的字串 / DATETIME: 日期時間 / TEXT: 長篇文字
TARGET_TABLE_COLUMNS = """
    column_id VARCHAR(255) PRIMARY KEY,
    column_name VARCHAR(255),
    table_name VARCHAR(255),
    column_type VARCHAR(255),
    definition LONGTEXT,
    sql_expression LONGTEXT,
    source_sql_fragment LONGTEXT
"""
# ==============================================================================


# 啟用 Logger
logger = logging.getLogger("mysql_table_setup")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
if not logger.handlers:
    logger.addHandler(console_handler)

def main():
    logger.info(f"開始執行 MySQL 資料表設定 (在 {TARGET_DB_NAME} 建立 {TARGET_TABLE_NAME})...")
    
    # 透過 mysql_connect 模組取得連線
    conn = mysql_connect.get_mysql_connection()
    
    if not conn:
        logger.error("無法取得資料庫連線，程式結束。")
        return

    try:
        cursor = conn.cursor()
        
        # 1. 先切換到你指定的資料庫 (USE database_name;)
        use_db_query = f"USE {TARGET_DB_NAME};"
        logger.info(f"切換資料庫: {use_db_query}")
        cursor.execute(use_db_query)
        
        # 2. 建立資料表的 SQL 語法 (IF NOT EXISTS 可以避免資料表已存在時報錯)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE_NAME} (
                {TARGET_TABLE_COLUMNS}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        
        logger.info(f"執行建立資料表 SQL:\n{create_table_query}")
        cursor.execute(create_table_query)
        
        # 3. 提交變更 (有時候建表需要 commit，依據 MySQL 設定而定，通常 DDL 語法會自動 commit，但加上比較保險)
        conn.commit()
        
        logger.info(f"成功確認/建立資料表: '{TARGET_TABLE_NAME}'")
        
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
