"""
Unit tests for StorageSQLLite implementation.
"""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from utils.Args import Args
from utils.Logger import Logger
from storage.StorageSQLLite import StorageSQLLite


class TestStorageSQLLite(unittest.TestCase):
    """Test cases for StorageSQLLite class."""
    
    def setUp(self) -> None:
        """Set up test environment before each test."""
        import sys
        # Save and restore original argv to prevent test interference
        self._original_argv = sys.argv.copy()
        # Set minimal argv (module required by Args)
        sys.argv = ["test", "noop"]
        
        # Initialize Args and Logger (required by Storage)
        Args.initialize()
        Logger.initialize(log_level="WARNING")  # Reduce log noise during tests
        
        # Create temporary database file
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_db_path = self.temp_dir / "test_drp_pipeline.db"
        
        # Create storage instance
        self.storage = StorageSQLLite()
    
    def tearDown(self) -> None:
        """Clean up after each test."""
        import sys
        self.storage.close()
        # Restore original argv
        sys.argv = self._original_argv
        # Clean up temp directory
        if self.temp_dir.exists():
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def test_initialize_default_path(self) -> None:
        """Test Storage initialization with default path."""
        # Change to temp directory for default path test
        import os
        original_cwd = os.getcwd()
        storage = None
        try:
            os.chdir(self.temp_dir)
            storage = StorageSQLLite()
            storage.initialize()
            self.assertTrue(storage._initialized)
            self.assertIsNotNone(storage._connection)
            default_path = Path.cwd() / "drp_pipeline.db"
            self.assertEqual(storage.get_db_path(), default_path)
            self.assertTrue(default_path.exists())
        finally:
            if storage:
                storage.close()
            os.chdir(original_cwd)
    
    def test_initialize_custom_path(self) -> None:
        """Test Storage initialization with custom path."""
        self.storage.initialize(db_path=self.test_db_path)
        self.assertTrue(self.storage._initialized)
        self.assertIsNotNone(self.storage._connection)
        self.assertEqual(self.storage.get_db_path(), self.test_db_path)
        self.assertTrue(self.test_db_path.exists())
    
    def test_initialize_creates_schema(self) -> None:
        """Test that initialization creates the projects table."""
        self.storage.initialize(db_path=self.test_db_path)
        
        # Check that table exists using direct SQL access
        cursor = self.storage._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='projects'"
        )
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "projects")
        
        # Check that indexes exist
        cursor = self.storage._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        )
        indexes = [row[0] for row in cursor.fetchall()]
        self.assertIn("idx_source_url", indexes)
        self.assertIn("idx_datalumos_id", indexes)
        self.assertIn("idx_status", indexes)
    
    def test_initialize_enables_wal_mode(self) -> None:
        """Test that WAL mode is enabled for concurrent access."""
        self.storage.initialize(db_path=self.test_db_path)
        
        cursor = self.storage._connection.execute("PRAGMA journal_mode")
        result = cursor.fetchone()
        self.assertEqual(result[0].upper(), "WAL")
    
    def test_initialize_idempotent(self) -> None:
        """Test that initialize can be called multiple times safely."""
        self.storage.initialize(db_path=self.test_db_path)
        first_connection = self.storage._connection
        
        self.storage.initialize(db_path=self.test_db_path)
        self.assertEqual(self.storage._connection, first_connection)
    
    def test_create_record(self) -> None:
        """Test creating a new record."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        self.assertEqual(drpid, 1)  # First auto-increment value
        
        # Verify record exists
        record = self.storage.get(drpid)
        self.assertIsNotNone(record)
        self.assertEqual(record["DRPID"], 1)
        self.assertEqual(record["source_url"], "https://example.com")
    
    def test_create_record_returns_drpid(self) -> None:
        """Test that create_record returns the DRPID."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid1 = self.storage.create_record("https://example1.com")
        drpid2 = self.storage.create_record("https://example2.com")
        drpid3 = self.storage.create_record("https://example3.com")
        
        self.assertEqual(drpid1, 1)
        self.assertEqual(drpid2, 2)
        self.assertEqual(drpid3, 3)
    
    def test_create_record_duplicate_source_url(self) -> None:
        """Test that duplicate source_url raises error."""
        self.storage.initialize(db_path=self.test_db_path)
        
        self.storage.create_record("https://example.com")
        
        with self.assertRaises(sqlite3.IntegrityError):
            self.storage.create_record("https://example.com")
    
    def test_get_record(self) -> None:
        """Test getting a record by DRPID."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        record = self.storage.get(drpid)
        self.assertIsNotNone(record)
        self.assertEqual(record["DRPID"], drpid)
        self.assertEqual(record["source_url"], "https://example.com")
    
    def test_get_record_not_found(self) -> None:
        """Test getting a non-existent record returns None."""
        self.storage.initialize(db_path=self.test_db_path)
        
        record = self.storage.get(999)
        self.assertIsNone(record)
    
    def test_exists_by_source_url_empty_db(self) -> None:
        """Test exists_by_source_url returns False when database is empty."""
        self.storage.initialize(db_path=self.test_db_path)
        
        self.assertFalse(self.storage.exists_by_source_url("https://example.com"))
    
    def test_exists_by_source_url_not_found(self) -> None:
        """Test exists_by_source_url returns False when URL not in database."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://existing.com")
        
        self.assertFalse(self.storage.exists_by_source_url("https://other.com"))
    
    def test_exists_by_source_url_found(self) -> None:
        """Test exists_by_source_url returns True when URL exists."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://example.com")
        
        self.assertTrue(self.storage.exists_by_source_url("https://example.com"))
    
    def test_get_record_only_non_null_values(self) -> None:
        """Test that get returns only non-null values."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        record = self.storage.get(drpid)
        # Should only have DRPID and source_url (folder_path is null)
        self.assertIn("DRPID", record)
        self.assertIn("source_url", record)
        self.assertNotIn("folder_path", record)
        self.assertNotIn("title", record)
    
    def test_update_record(self) -> None:
        """Test updating a record."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        # Update with multiple fields
        self.storage.update_record(drpid, {
            "title": "My Project",
            "status": "sourced",
            "folder_path": "C:\\data\\project1"
        })
        
        record = self.storage.get(drpid)
        self.assertEqual(record["title"], "My Project")
        self.assertEqual(record["status"], "sourced")
        self.assertEqual(record["folder_path"], "C:\\data\\project1")
    
    def test_update_record_partial(self) -> None:
        """Test updating only some fields."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        # Set initial values
        self.storage.update_record(drpid, {"title": "Initial Title", "status": "collected"})
        
        # Update only one field
        self.storage.update_record(drpid, {"status": "published"})
        
        record = self.storage.get(drpid)
        self.assertEqual(record["title"], "Initial Title")  # Should remain
        self.assertEqual(record["status"], "published")  # Should be updated
    
    def test_update_record_not_exist(self) -> None:
        """Test updating non-existent record raises ValueError."""
        self.storage.initialize(db_path=self.test_db_path)
        
        with self.assertRaises(ValueError) as cm:
            self.storage.update_record(999, {"title": "Test"})
        self.assertIn("does not exist", str(cm.exception))
    
    def test_update_record_cannot_update_drpid(self) -> None:
        """Test that DRPID cannot be updated."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        with self.assertRaises(ValueError) as cm:
            self.storage.update_record(drpid, {"DRPID": 999})
        self.assertIn("Cannot update DRPID", str(cm.exception))
    
    def test_update_record_cannot_update_source_url(self) -> None:
        """Test that source_url cannot be updated."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        with self.assertRaises(ValueError) as cm:
            self.storage.update_record(drpid, {"source_url": "https://other.com"})
        self.assertIn("Cannot update", str(cm.exception))
        self.assertIn("source_url", str(cm.exception))
    
    def test_update_record_invalid_column(self) -> None:
        """Test that invalid columns raise sqlite3.OperationalError."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        # Database will raise error for invalid column
        with self.assertRaises(sqlite3.OperationalError) as cm:
            self.storage.update_record(drpid, {"invalid_column": "value"})
        self.assertIn("no such column", str(cm.exception))
    
    def test_update_record_empty_dict(self) -> None:
        """Test that empty update dict does nothing."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        # Should not raise error
        self.storage.update_record(drpid, {})
        
        record = self.storage.get(drpid)
        self.assertEqual(record["source_url"], "https://example.com")
    
    def test_delete_record(self) -> None:
        """Test deleting a record."""
        self.storage.initialize(db_path=self.test_db_path)
        
        drpid = self.storage.create_record("https://example.com")
        
        # Verify exists
        record = self.storage.get(drpid)
        self.assertIsNotNone(record)
        
        # Delete
        self.storage.delete(drpid)
        
        # Verify deleted
        record = self.storage.get(drpid)
        self.assertIsNone(record)
    
    def test_delete_record_not_exist(self) -> None:
        """Test deleting non-existent record raises ValueError."""
        self.storage.initialize(db_path=self.test_db_path)
        
        with self.assertRaises(ValueError) as cm:
            self.storage.delete(999)
        self.assertIn("does not exist", str(cm.exception))
    
    def test_get_db_path(self) -> None:
        """Test getting database path."""
        self.storage.initialize(db_path=self.test_db_path)
        self.assertEqual(self.storage.get_db_path(), self.test_db_path)
    
    def test_get_db_path_not_initialized(self) -> None:
        """Test that get_db_path raises error when not initialized."""
        with self.assertRaises(RuntimeError):
            self.storage.get_db_path()
    
    def test_close_connection(self) -> None:
        """Test closing the database connection."""
        self.storage.initialize(db_path=self.test_db_path)
        self.assertTrue(self.storage._initialized)
        
        self.storage.close()
        self.assertFalse(self.storage._initialized)
        self.assertIsNone(self.storage._connection)
    
    def test_clear_all_records(self) -> None:
        """Test clearing all records from the database and resetting auto-increment."""
        self.storage.initialize(db_path=self.test_db_path)
        
        # Create multiple records
        drpid1 = self.storage.create_record("https://example.com/1")
        drpid2 = self.storage.create_record("https://example.com/2")
        drpid3 = self.storage.create_record("https://example.com/3")
        
        # Verify records exist and IDs are sequential
        self.assertEqual(drpid1, 1)
        self.assertEqual(drpid2, 2)
        self.assertEqual(drpid3, 3)
        self.assertIsNotNone(self.storage.get(drpid1))
        self.assertIsNotNone(self.storage.get(drpid2))
        self.assertIsNotNone(self.storage.get(drpid3))
        
        # Clear all records
        self.storage.clear_all_records()
        
        # Verify all records are gone
        self.assertIsNone(self.storage.get(drpid1))
        self.assertIsNone(self.storage.get(drpid2))
        self.assertIsNone(self.storage.get(drpid3))
        
        # Verify list_eligible_projects returns empty
        projects = self.storage.list_eligible_projects(None, None)
        self.assertEqual(len(projects), 0)
        
        # Verify auto-increment was reset - new record should get ID 1
        new_drpid = self.storage.create_record("https://example.com/new")
        self.assertEqual(new_drpid, 1, "Auto-increment should reset to 1 after clearing")
    
    def test_schema_all_fields(self) -> None:
        """Test that schema includes all required fields."""
        self.storage.initialize(db_path=self.test_db_path)
        
        cursor = self.storage._connection.execute("PRAGMA table_info(projects)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        # Check all required fields exist
        required_fields = [
            "DRPID", "source_url", "folder_path", "title", "agency", "office",
            "summary", "keywords", "time_start", "time_end", "data_types",
            "extensions", "download_date", "collection_notes", "file_size",
            "datalumos_id",
            "published_url", "status", "status_notes", "warnings", "errors"
        ]
        
        for field in required_fields:
            self.assertIn(field, columns, f"Field {field} missing from schema")
        
        # Verify file_size is TEXT, not INTEGER
        self.assertEqual(columns["file_size"], "TEXT")
        
        # Verify DRPID is INTEGER
        self.assertEqual(columns["DRPID"], "INTEGER")
    
    def test_concurrent_access_simulation(self) -> None:
        """Test that database can handle multiple operations (simulating concurrency)."""
        self.storage.initialize(db_path=self.test_db_path)
        
        # Create multiple records
        drpids = []
        for i in range(10):
            drpid = self.storage.create_record(f"https://example{i}.com")
            drpids.append(drpid)
        
        # Verify all created
        self.assertEqual(len(drpids), 10)
        self.assertEqual(drpids, list(range(1, 11)))
    
    def test_drpid_auto_increment(self) -> None:
        """Test that DRPID auto-increments."""
        self.storage.initialize(db_path=self.test_db_path)
        
        # Create multiple records
        drpid1 = self.storage.create_record("https://example1.com")
        drpid2 = self.storage.create_record("https://example2.com")
        drpid3 = self.storage.create_record("https://example3.com")
        
        # Verify DRPID values
        self.assertEqual(drpid1, 1)
        self.assertEqual(drpid2, 2)
        self.assertEqual(drpid3, 3)
    
    def test_source_url_unique_constraint(self) -> None:
        """Test that source_url must be unique."""
        self.storage.initialize(db_path=self.test_db_path)
        
        self.storage.create_record("https://example.com")
        
        with self.assertRaises(sqlite3.IntegrityError):
            self.storage.create_record("https://example.com")
    
    def test_datalumos_id_unique_constraint(self) -> None:
        """Test that datalumos_id must be unique when provided."""
        self.storage.initialize(db_path=self.test_db_path)
        
        self.storage.create_record("https://example1.com")
        self.storage.update_record(1, {"datalumos_id": "DL123"})
        
        # Try to set duplicate datalumos_id - should fail
        self.storage.create_record("https://example2.com")
        with self.assertRaises(sqlite3.IntegrityError):
            self.storage.update_record(2, {"datalumos_id": "DL123"})
    
    def test_datalumos_id_nullable(self) -> None:
        """Test that datalumos_id can be null."""
        self.storage.initialize(db_path=self.test_db_path)
        
        # Create without datalumos_id
        drpid1 = self.storage.create_record("https://example.com")
        
        # Create with datalumos_id
        drpid2 = self.storage.create_record("https://example2.com")
        self.storage.update_record(drpid2, {"datalumos_id": "DL123"})
        
        # Both should succeed
        record1 = self.storage.get(drpid1)
        record2 = self.storage.get(drpid2)
        
        self.assertNotIn("datalumos_id", record1)  # Not in dict because null
        self.assertEqual(record2["datalumos_id"], "DL123")
    
    def test_folder_path_nullable(self) -> None:
        """Test that folder_path can be null."""
        self.storage.initialize(db_path=self.test_db_path)
        
        # Create without folder_path
        drpid = self.storage.create_record("https://example.com")
        
        # Verify it's not in the returned dict (because it's null)
        record = self.storage.get(drpid)
        self.assertNotIn("folder_path", record)
    
    def test_methods_not_initialized(self) -> None:
        """Test that methods raise error when not initialized."""
        storage = StorageSQLLite()
        
        with self.assertRaises(RuntimeError):
            storage.create_record("https://example.com")
        
        with self.assertRaises(RuntimeError):
            storage.exists_by_source_url("https://example.com")
        
        with self.assertRaises(RuntimeError):
            storage.get(1)
        
        with self.assertRaises(RuntimeError):
            storage.update_record(1, {"title": "Test"})
        
        with self.assertRaises(RuntimeError):
            storage.delete(1)

    def test_list_eligible_projects_none_prereq_returns_empty(self) -> None:
        """Test list_eligible_projects with prereq_status None returns []."""
        self.storage.initialize(db_path=self.test_db_path)
        out = self.storage.list_eligible_projects(None, 10)
        self.assertEqual(out, [])

    def test_list_eligible_projects_filters_status_and_errors(self) -> None:
        """Test list_eligible_projects returns only status=X and (errors null or '')."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://a.com")
        self.storage.update_record(1, {"status": "sourced"})
        self.storage.create_record("https://b.com")
        self.storage.update_record(2, {"status": "sourced", "errors": "fail"})
        self.storage.create_record("https://c.com")
        self.storage.update_record(3, {"status": "sourced"})
        out = self.storage.list_eligible_projects("sourced", None)
        self.assertEqual(len(out), 2)
        drpids = [r["DRPID"] for r in out]
        self.assertIn(1, drpids)
        self.assertIn(3, drpids)
        self.assertNotIn(2, drpids)

    def test_list_eligible_projects_respects_limit(self) -> None:
        """Test list_eligible_projects respects limit."""
        self.storage.initialize(db_path=self.test_db_path)
        for i in range(5):
            self.storage.create_record(f"https://x{i}.com")
            self.storage.update_record(i + 1, {"status": "sourced"})
        out = self.storage.list_eligible_projects("sourced", 2)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["DRPID"], 1)
        self.assertEqual(out[1]["DRPID"], 2)

    def test_list_eligible_projects_respects_start_row(self) -> None:
        """Test list_eligible_projects skips first (start_row - 1) rows of full table."""
        self.storage.initialize(db_path=self.test_db_path)
        for i in range(5):
            self.storage.create_record(f"https://example.com/{i}")
            self.storage.update_record(i + 1, {"status": "sourced"})
        out = self.storage.list_eligible_projects("sourced", None, start_row=3)
        self.assertEqual(len(out), 3)
        self.assertEqual([r["DRPID"] for r in out], [3, 4, 5])

    def test_list_eligible_projects_start_row_counts_all_rows(self) -> None:
        """Test start_row uses full table row count, not just eligible rows."""
        self.storage.initialize(db_path=self.test_db_path)
        for i in range(5):
            self.storage.create_record(f"https://example.com/{i}")
        self.storage.update_record(1, {"status": "sourced"})
        self.storage.update_record(3, {"status": "sourced"})
        self.storage.update_record(5, {"status": "sourced"})
        # Rows 2,4 have other status. Full table: 1,2,3,4,5. start_row=4 -> DRPID>=4
        out = self.storage.list_eligible_projects("sourced", None, start_row=4)
        self.assertEqual([r["DRPID"] for r in out], [5])

    def test_list_eligible_projects_order_by_drpid(self) -> None:
        """Test list_eligible_projects returns rows ordered by DRPID ASC."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://a.com")
        self.storage.update_record(1, {"status": "sourced"})
        self.storage.create_record("https://b.com")
        self.storage.update_record(2, {"status": "sourced"})
        out = self.storage.list_eligible_projects("sourced", None)
        self.assertEqual([r["DRPID"] for r in out], [1, 2])

    def test_list_eligible_projects_respects_min_drpid(self) -> None:
        """Test list_eligible_projects with min_drpid returns only DRPID >= value."""
        self.storage.initialize(db_path=self.test_db_path)
        for i in range(5):
            self.storage.create_record(f"https://example.com/{i}")
            self.storage.update_record(i + 1, {"status": "sourced"})
        out = self.storage.list_eligible_projects("sourced", None, min_drpid=4)
        self.assertEqual([r["DRPID"] for r in out], [4, 5])

    def test_append_to_field_warnings(self) -> None:
        """Test append_to_field appends to warnings with newline."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://a.com")
        self.storage.append_to_field(1, "warnings", "w1")
        r = self.storage.get(1)
        self.assertEqual(r.get("warnings"), "w1")
        self.storage.append_to_field(1, "warnings", "w2")
        r = self.storage.get(1)
        self.assertEqual(r["warnings"], "w1\nw2")

    def test_append_to_field_errors(self) -> None:
        """Test append_to_field appends to errors."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://a.com")
        self.storage.append_to_field(1, "errors", "e1")
        r = self.storage.get(1)
        self.assertEqual(r["errors"], "e1")

    def test_append_to_field_invalid_field_raises(self) -> None:
        """Test append_to_field with field not warnings/errors raises."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://a.com")
        with self.assertRaises(ValueError) as cm:
            self.storage.append_to_field(1, "status", "x")
        self.assertIn("warnings", str(cm.exception))
        self.assertIn("errors", str(cm.exception))

    def test_append_to_field_nonexistent_drpid_raises(self) -> None:
        """Test append_to_field for missing DRPID raises."""
        self.storage.initialize(db_path=self.test_db_path)
        with self.assertRaises(ValueError) as cm:
            self.storage.append_to_field(999, "warnings", "x")
        self.assertIn("does not exist", str(cm.exception))

    def test_append_to_field_preserves_whitespace(self) -> None:
        """Test append_to_field preserves whitespace consistently across multiple appends."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://a.com")
        # First append with leading/trailing whitespace
        self.storage.append_to_field(1, "warnings", " warning1 ")
        r = self.storage.get(1)
        self.assertEqual(r["warnings"], " warning1 ")
        # Second append should preserve leading whitespace from first entry
        # (trailing whitespace from entire field is stripped to avoid trailing newlines)
        self.storage.append_to_field(1, "warnings", " warning2 ")
        r = self.storage.get(1)
        # Leading spaces preserved, trailing space from last entry stripped
        self.assertEqual(r["warnings"], " warning1 \n warning2")

    def test_list_records_with_status_notes(self) -> None:
        """Test list_records_with_status_notes returns only records with non-empty status_notes."""
        self.storage.initialize(db_path=self.test_db_path)
        self.storage.create_record("https://a.com")
        self.storage.create_record("https://b.com")
        self.storage.create_record("https://c.com")
        self.storage.update_record(1, {"status_notes": "  CSV -> csv\n  JSON -> 404"})
        self.storage.update_record(3, {"status_notes": "  X -> html https://x.com"})
        out = self.storage.list_records_with_status_notes()
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["DRPID"], 1)
        self.assertIn("CSV", out[0]["status_notes"])
        self.assertEqual(out[1]["DRPID"], 3)
        self.assertIn("https://x.com", out[1]["status_notes"])
