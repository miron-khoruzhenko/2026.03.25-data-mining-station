import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.db.db_manager import DBManager

class ExcelExporter:
    def __init__(self):
        self.db = DBManager()
        self.exports_dir = PROJECT_ROOT / "data" / "exports"
        self.exports_dir.mkdir(parents=True, exist_ok=True)

    def export_done_items(self, filename_prefix="export"):
        with self.db.get_connection() as conn:
            df = pd.read_sql_query("SELECT id, data_url, source_url, extracted_data FROM scraper_items WHERE status = 'done'", conn)

        if df.empty: return None

        expanded_rows = []
        for _, row in df.iterrows():
            base = {'data_url': row['data_url'], 'category': row['source_url']}
            if row['extracted_data']:
                try:
                    data = json.loads(row['extracted_data'])
                    base.update(data)
                except json.JSONDecodeError:
                    pass
            expanded_rows.append(base)

        final_df = pd.DataFrame(expanded_rows)

        base_columns = ['company_name', 'company_phone', 'company_email', 'company_website', 'company_fax']
        system_columns = ['data_url', 'source_url', 'category']
        all_columns = list(final_df.columns)
        custom_columns = [col for col in all_columns if col not in base_columns and col not in system_columns]
        
        final_columns = [col for col in base_columns if col in all_columns]
        final_columns.extend(custom_columns)
        final_columns.extend([col for col in system_columns if col in all_columns])

        final_df = final_df[final_columns]

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filepath = self.exports_dir / f"{filename_prefix}_{timestamp}.xlsx"

        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                grouped = final_df.groupby('category')
                for category_name, group_df in grouped:
                    safe_sheet_name = str(category_name)[:31].replace('/', '_').replace('\\', '_').replace('?', '_')
                    group_df.drop(columns=['category']).to_excel(writer, sheet_name=safe_sheet_name, index=False)
            return filepath
        except Exception:
            return None