import logging
import smtplib
from email.mime.text import MIMEText
import pandas as pd
from db_controller import get_table_columns, backup_table
from datetime import datetime

class ValidationReport:
    """Accumulates success and failure messages and sends a single summary email."""
    def __init__(self, config):
        self.success_messages: list[str] = []
        self.failure_messages: list[str] = []
        self.config = config

    def add_success(self, message: str):
        self.success_messages.append(message)

    def add_failure(self, message: str):
        self.failure_messages.append(message)

    def _has_activity(self) -> bool:
        return bool(self.success_messages or self.failure_messages)

    def send_report_if_needed(self):
        # Send a report if any files were processed (success or failure collected)
        if not self._has_activity():
            logging.info("No files processed. No report to send.")
            return

        has_failures = bool(self.failure_messages)
        subject = "[CSGP Import Report] " + ("Failures Detected" if has_failures else "Success")

        sections = []
        if self.success_messages:
            html_success = "<h2>=== Successful Imports ===</h2>"
            for msg in self.success_messages:
                html_success += f"<pre style='font-family: monospace; white-space: pre-wrap;'>{msg}</pre><hr>"
            sections.append(html_success)
            
        if self.failure_messages:
            html_failure = "<h2>=== Skipped / Failed ===</h2>"
            for msg in self.failure_messages:
                if "</table>" in msg:
                    # Message contains HTML table
                    html_failure += f"<div style='margin-bottom: 20px;'>{msg}</div><hr>"
                else:
                    html_failure += f"<pre style='font-family: monospace; white-space: pre-wrap;'>{msg}</pre><hr>"
            sections.append(html_failure)
            
        full_body = f"<html><body>{''.join(sections)}</body></html>"

        try:
            if not self.config.has_section('EMAIL') or not all(k in self.config['EMAIL'] for k in ['SMTP_SERVER', 'SMTP_PORT', 'SENDER', 'RECIPIENT']):
                logging.warning("Email configuration is incomplete. Cannot send report. See accumulated messages below:")
                logging.warning(full_body)
                return

            email_config = self.config['EMAIL']
            msg = MIMEText(full_body, 'html')
            msg['Subject'] = subject
            msg['From'] = email_config['SENDER']
            msg['To'] = email_config['RECIPIENT']

            with smtplib.SMTP(email_config['SMTP_SERVER'], int(email_config['SMTP_PORT'])) as server:
                if 'SMTP_USER' in email_config and email_config['SMTP_USER'] and 'SMTP_PASSWORD' in email_config and email_config['SMTP_PASSWORD']:
                    server.starttls()
                    server.login(email_config['SMTP_USER'], email_config['SMTP_PASSWORD'])
                server.send_message(msg)
            logging.info(f"Successfully sent validation summary email to {email_config['RECIPIENT']}.")
        except Exception as e:
            logging.error(f"Failed to send summary email: {e}")
            logging.error(f"Accumulated Messages:\n{full_body}")


def run_validations(df, table_name, engine, config, report):
    """
    Runs all validation checks.
    Returns True if the data can be imported, False otherwise.
    Validation messages are added to the report object.
    """
    
    # 1. Check for empty DataFrame
    if df.empty:
        report.add_failure(f"--- Table: {table_name} ---\n"
                           f"Result: SKIPPED\n"
                           f"Reason: The Excel file contains no data rows.")
        return False

    # 2. Check for column consistency
    try:
        db_columns = get_table_columns(table_name, engine)
        if db_columns:
            # 來自 Excel 的欄位
            df_columns = set(col.strip().replace(' ', '_').lower() for col in df.columns)
            
            # 來自資料庫的欄位，但排除 'dw_ins_time' 後再進行比對
            db_columns_for_comparison = set(db_columns) - {'dw_ins_time'}

            if df_columns != db_columns_for_comparison:
                backup_name = f"{table_name}_bk_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                backup_table(table_name, backup_name, engine)
                report.add_success(f"--- Table: {table_name} ---\n"
                                   f"Action: PROCEEDED (with backup)\n"
                                   f"Detail: Column mismatch detected. Existing table was backed up to '{backup_name}' and a new table will be created.")
    except Exception as e:
        report.add_failure(f"--- Table: {table_name} ---\n"
                           f"Result: SKIPPED\n"
                           f"Reason: A critical database error occurred during column validation: {e}")
        return False

    # 3. Check for dimension conflicts (same keys with different value/amounts)
    #    VALIDATION_KEYS lists dimension columns; we must ensure for each dimension tuple,
    #    there is not more than one distinct set of value columns (prices/rebates/fees).
    if config.has_option('VALIDATION_KEYS', table_name):
        raw_key_groups = config.get('VALIDATION_KEYS', table_name)
        if raw_key_groups:
            # 支援多組 Key，以 '|' 分隔，由左至右檢查
            key_groups = [group.strip() for group in raw_key_groups.split('|') if group.strip()]
            
            # Standardize df columns once before checking
            df.columns = [col.strip().replace(' ', '_').lower() for col in df.columns]

            is_valid_all_groups = True
            for group_idx, group_str in enumerate(key_groups, start=1):
                key_columns = [k.strip() for k in group_str.split(',') if k.strip()]

                if all(k in df.columns for k in key_columns):
                    # 排除 Key 欄位含有空值 (NaN 或空字串) 的資料再進行檢查
                    # 我們只檢查「有值」的情況下是否有重複
                    non_blank_mask = df[key_columns].notna().all(axis=1)
                    for k in key_columns:
                        non_blank_mask &= (df[k].astype(str).str.strip() != '')
                    
                    target_df = df[non_blank_mask]
                    duplicate_mask = target_df.duplicated(subset=key_columns, keep=False)
                    
                    if duplicate_mask.any():
                        is_valid_all_groups = False
                        conflicts_df = target_df[duplicate_mask].sort_values(by=key_columns)
                        
                        lines = [f"<b>--- Table: {table_name} (Round {group_idx}) ---</b><br>",
                                 f"Result: ERROR<br>",
                                 f"Reason: Key Violation - Duplicated keys found.<br>",
                                 f"Validation Keys: {key_columns}<br>",
                                 f"Total records found with duplication: {len(conflicts_df)}<br>"]
                        
                        display_limit = 100
                        with pd.option_context('display.max_columns', None, 'display.width', 1000, 'display.max_colwidth', None):
                            if len(conflicts_df) <= display_limit:
                                lines.append("<br>Full list of duplicated records:<br>")
                                # 使用 to_html 產生表格，加入一點樣式
                                lines.append(conflicts_df.to_html(index=False, border=1, classes='table table-bordered table-striped'))
                            else:
                                sample_conflicts = conflicts_df.head(display_limit)
                                lines.append(f"<br>Sample of first {display_limit} duplicated records:<br>")
                                lines.append(sample_conflicts.to_html(index=False, border=1, classes='table table-bordered table-striped'))
                                lines.append(f"<br>... and {len(conflicts_df) - display_limit} more duplicated rows not shown.")
                        
                        report.add_failure("".join(lines))
                        # 注意：這裡不再 return False，而是繼續循環下一組檢查
                else:
                    missing_keys = [k for k in key_columns if k not in df.columns]
                    logging.warning(
                        f"Could not perform uniqueness check for {table_name} (Group #{group_idx}): missing key columns {missing_keys}. "
                        f"DataFrame columns: {df.columns.tolist()}"
                    )
            
            if not is_valid_all_groups:
                return False

    return True

