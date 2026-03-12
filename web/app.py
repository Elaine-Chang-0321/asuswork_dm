import os
import sys
from pathlib import Path
from flask import Flask, render_template, request, jsonify

# 將上層目錄加到 sys.path，以便載入 mysql_connect (與 Vault 等模組)
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import mysql_connect

app = Flask(__name__)

def get_db():
    try:
        conn = mysql_connect.get_mysql_connection()
        return conn
    except Exception as e:
        print(f"資料庫連線錯誤: {e}")
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/models')
def api_models():
    """取得所有不重複的 model_name 清單"""
    conn = get_db()
    if not conn:
        return jsonify({"error": "DB connection failed"}), 500
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT model_name FROM dataserviceflow_dm.columnlist ORDER BY model_name")
        rows = cursor.fetchall()
        models = [row[0] for row in rows if row[0]]
        cursor.close()
        conn.close()
        return jsonify({"models": models})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/find_model')
def api_find_model():
    """給定 table_name，反查它屬於哪一個 model_name"""
    table_name = request.args.get('table')
    if not table_name:
        return jsonify({"model": None})
        
    conn = get_db()
    if not conn:
        return jsonify({"error": "DB connection failed"}), 500
    try:
        cursor = conn.cursor()
        # 先找完全一樣的名稱
        cursor.execute("SELECT model_name FROM dataserviceflow_dm.columnlist WHERE table_name = %s LIMIT 1", (table_name,))
        row = cursor.fetchone()
        
        # 如果沒找到，而且有類似 "ods.table_name" 的前綴，拔掉前綴再找一次
        exact_table_name = table_name
        if not row and '.' in table_name:
            exact_table_name = table_name.split('.')[-1]
            cursor.execute("SELECT model_name FROM dataserviceflow_dm.columnlist WHERE table_name = %s LIMIT 1", (exact_table_name,))
            row = cursor.fetchone()
            
        cursor.close()
        conn.close()
        
        if row:
            return jsonify({"model": row[0], "exact_table_name": exact_table_name})
        else:
            return jsonify({"model": None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/tables')
def api_tables():
    """根據選定的 model_name，取得該 Model 產出的所有 table_name"""
    model_name = request.args.get('model')
    if not model_name:
        return jsonify({"tables": []})
        
    conn = get_db()
    if not conn:
        return jsonify({"error": "DB connection failed"}), 500
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT table_name FROM dataserviceflow_dm.columnlist WHERE model_name = %s ORDER BY table_name", (model_name,))
        rows = cursor.fetchall()
        tables = [row[0] for row in rows if row[0]]
        cursor.close()
        conn.close()
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/columns')
def api_columns():
    """根據選定的 model_name 和 table_name，取得所有的欄位細節"""
    model_name = request.args.get('model')
    table_name = request.args.get('table')
    if not model_name or not table_name:
        return jsonify({"columns": []})
        
    conn = get_db()
    if not conn:
        return jsonify({"error": "DB connection failed"}), 500
    try:
        cursor = conn.cursor()
        query = """
            SELECT column_name, column_type, source_table, sql_expression, source_sql_fragment, dw_ins_time
            FROM dataserviceflow_dm.columnlist 
            WHERE model_name = %s AND table_name = %s
            ORDER BY column_name
        """
        cursor.execute(query, (model_name, table_name))
        
        columns = []
        for r in cursor.fetchall():
            columns.append({
                "column_name": r[0] if r[0] else "",
                "column_type": r[1] if r[1] else "",
                "source_table": r[2] if r[2] else "",
                "sql_expression": r[3] if r[3] else "",
                "source_sql_fragment": r[4] if r[4] else "",
                "dw_ins_time": str(r[5]) if r[5] else ""
            })
            
        cursor.close()
        conn.close()
        return jsonify({"columns": columns})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # 確保連同 .env 也能順利載入
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    
    print("[INFO] 正在啟動 Flask 網頁伺服器...")
    print("[INFO] 請打開瀏覽器並前往 --> http://127.0.0.1:5000")
    # debug=True 可以在存檔時自動重啟伺服器
    app.run(host='0.0.0.0', port=5000, debug=True)
