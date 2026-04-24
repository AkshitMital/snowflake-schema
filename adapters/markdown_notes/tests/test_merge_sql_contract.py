from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


class MergeSqlContractTests(unittest.TestCase):
    def test_required_sql_files_exist_and_reference_merge_order(self) -> None:
        sql_root = ROOT / "sql"
        aps_raw_sql = Path("sql/aps_raw/001_content_adapter_stage_tables.sql")
        merge_items_sql = sql_root / "002_merge_content_items.sql"
        merge_versions_sql = sql_root / "003_merge_content_versions.sql"
        merge_children_sql = sql_root / "004_merge_content_children.sql"
        validation_sql = sql_root / "001_pre_merge_validation.sql"

        self.assertTrue(aps_raw_sql.exists())
        self.assertTrue(validation_sql.exists())
        self.assertTrue(merge_items_sql.exists())
        self.assertTrue(merge_versions_sql.exists())
        self.assertTrue(merge_children_sql.exists())

        merge_versions_text = merge_versions_sql.read_text(encoding="utf-8")
        self.assertIn("BEGIN;", merge_versions_text)
        self.assertIn("MERGE INTO APS_CORE.CONTENT_VERSIONS", merge_versions_text)
        self.assertIn("MERGE INTO APS_CORE.CONTENT_ITEM_CURRENT_VERSION", merge_versions_text)
        self.assertIn("COMMIT;", merge_versions_text)


if __name__ == "__main__":
    unittest.main()

