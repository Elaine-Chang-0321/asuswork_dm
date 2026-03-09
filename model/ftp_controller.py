import ftplib
import os
import logging
import sys
from npspo_vault_client import VaultClient

role_id=os.getenv("VAULT_CSGP_ROLE_ID")
secret_id=os.getenv("VAULT_CSGP_SECRET_ID")

# 初始化客戶端
client = VaultClient()
client.get_approle_token(role_id=role_id, secret_id=secret_id)

# 取得 Secret (以 aocc API Key 為例)
secret_data = client.get_vault_secret("/v1/ftp_secret/data/asusftps.asus.com/apza002hrc/cap%5CGP.ftp")
username = secret_data.get("Username")
password = secret_data.get("Password")

def download_files_from_ftp(ftp_config, download_dir):
    """
    Downloads .xlsx/.xlsm files from the FTP's PROCESSING_DIR to a local directory.
    Returns a list of local file paths of the downloaded files.
    """
    downloaded_files = []
    ftp = None
    try:
        ftp = ftplib.FTP_TLS()
        ftp.connect(ftp_config['HOST'])
        ftp.login(username, password)
        ftp.prot_p()
        
        # Force a safe control-connection encoding to avoid decode errors
        try:
            resp = ftp.sendcmd('OPTS UTF8 ON')
            if resp and resp.startswith('2'):
                ftp.encoding = 'utf-8'
            else:
                ftp.encoding = 'latin-1'
        except Exception:
            ftp.encoding = 'latin-1'
        
        ftp.cwd(ftp_config['PROCESSING_DIR'])
        files = ftp.nlst()
        
        for filename in files:
            if not filename.lower().endswith(('.xlsx', '.xlsm')):
                continue

            local_filepath = os.path.join(download_dir, filename)
            logging.info(f"Downloading {filename} to {local_filepath}...")
            with open(local_filepath, 'wb') as f:
                ftp.retrbinary('RETR ' + filename, f.write)
            
            downloaded_files.append(local_filepath)
        
        logging.info(f"Downloaded {len(downloaded_files)} files from FTP.")
        return downloaded_files
    except Exception as e:
        logging.error(f"An error occurred during FTP download: {e}")
        sys.exit(1)
    finally:
        if ftp:
            ftp.quit()

def move_file(ftp_config, filename, from_dir_key, to_dir_key):
    """Move a file between FTP directories. If destination exists, overwrite it."""
    ftp = None
    try:
        ftp = ftplib.FTP_TLS()
        ftp.connect(ftp_config['HOST'])
        ftp.login(username, password)
        ftp.prot_p()

        # Force a safe control-connection encoding to avoid decode errors
        try:
            resp = ftp.sendcmd('OPTS UTF8 ON')
            if resp and resp.startswith('2'):
                ftp.encoding = 'utf-8'
            else:
                ftp.encoding = 'latin-1'
        except Exception:
            ftp.encoding = 'latin-1'
            
        from_dir = ftp_config[from_dir_key]
        to_dir = ftp_config[to_dir_key]
        from_path = f"{from_dir}/{filename}"
        to_path = f"{to_dir}/{filename}"

        logging.info(f"Moving {from_path} to {to_path}...")

        # Always attempt to delete destination first to ensure overwrite works across servers
        try:
            ftp.delete(to_path)
            logging.info(f"Deleted existing destination file: {to_path}")
        except Exception:
            # This is fine, it just means the file doesn't exist at the destination
            pass

        # Perform the move (rename)
        ftp.rename(from_path, to_path)
        logging.info(f"Successfully moved {filename} to {to_dir_key}.")
            
    except Exception as e:
        logging.error(f"Unexpected error during FTP move of {filename} from {from_dir_key} to {to_dir_key}: {e}")
        sys.exit(1) # Re-raise to allow caller to handle it
    finally:
        if ftp:
            ftp.quit()

def move_files_to_processing(ftp_config):
    """Moves all .xlsx/.xlsm files from the UPLOAD_DIR to the PROCESSING_DIR with overwrite semantics."""
    ftp = None
    try:
        ftp = ftplib.FTP_TLS()
        ftp.connect(ftp_config['HOST'])
        ftp.login(username, password)
        ftp.prot_p()

        # Force a safe control-connection encoding to avoid decode errors
        try:
            resp = ftp.sendcmd('OPTS UTF8 ON')
            if resp and resp.startswith('2'):
                ftp.encoding = 'utf-8'
            else:
                ftp.encoding = 'latin-1'
        except Exception:
            ftp.encoding = 'latin-1'
            
        ftp.cwd(ftp_config['UPLOAD_DIR'])
        files = ftp.nlst()

        moved = 0
        for filename in files:
            if not filename.lower().endswith(('.xlsx', '.xlsm')):
                continue
            
            from_path = f"{ftp_config['UPLOAD_DIR']}/{filename}"
            to_path = f"{ftp_config['PROCESSING_DIR']}/{filename}"
            
            try:
                # 1. Attempt to delete destination
                try:
                    ftp.delete(to_path)
                    logging.info(f"Deleted existing destination file: {to_path}")
                except Exception:
                    # This is fine, it just means the file doesn't exist at the destination
                    pass

                # 2. Perform the rename
                ftp.rename(from_path, to_path)
                logging.info(f"Moved {from_path} to {to_path}")
                moved += 1
            except Exception as e:
                logging.error(f"Failed to move upload file {filename} to processing: {e}")
        
        logging.info(f"Moved {moved} file(s) from UPLOAD_DIR to PROCESSING_DIR.")
    except Exception as e:
        logging.error(f"An error occurred while moving files to processing: {e}")
        sys.exit(1)
    finally:
        if ftp:
            ftp.quit()
