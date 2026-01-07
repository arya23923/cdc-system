# File: test_cdc_system.py
"""
Unit tests for CDC system
Run with: python -m pytest test_cdc_system.py
"""

import pytest
import os
import tempfile
from cdc_system import CDCSystem, CDCReplicator, CDCMonitor, SQLiteConnection


class TestCDCSystem:
    """Test suite for CDC System"""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing"""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        yield path
        os.unlink(path)
    
    @pytest.fixture
    def setup_db(self, temp_db):
        """Setup test database with sample table"""
        db = SQLiteConnection(temp_db).connect()
        db.execute('''
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER NOT NULL
            )
        ''')
        db.commit()
        return db
    
    def test_cdc_setup(self, setup_db):
        """Test CDC infrastructure setup"""
        cdc = CDCSystem(setup_db, "test_table")
        cdc.setup_trigger_based_cdc(['id', 'name', 'value'])
        
        # Verify CDC table was created
        setup_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table_cdc'"
        )
        result = setup_db.fetchone()
        assert result is not None
    
    def test_insert_capture(self, setup_db):
        """Test capturing INSERT operations"""
        cdc = CDCSystem(setup_db, "test_table")
        cdc.setup_trigger_based_cdc(['id', 'name', 'value'])
        
        # Insert data
        setup_db.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", 
                        ("test1", 100))
        setup_db.commit()
        
        # Check CDC captured the insert
        changes = cdc.get_pending_changes()
        assert len(changes) == 1
        assert changes[0]['operation'] == 'INSERT'
        assert changes[0]['new_data']['name'] == 'test1'
    
    def test_update_capture(self, setup_db):
        """Test capturing UPDATE operations"""
        cdc = CDCSystem(setup_db, "test_table")
        cdc.setup_trigger_based_cdc(['id', 'name', 'value'])
        
        # Insert and update data
        setup_db.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", 
                        ("test1", 100))
        setup_db.commit()
        
        setup_db.execute("UPDATE test_table SET value = 200 WHERE name = 'test1'")
        setup_db.commit()
        
        # Check CDC captured both operations
        changes = cdc.get_pending_changes()
        assert len(changes) == 2
        assert changes[1]['operation'] == 'UPDATE'
        assert changes[1]['old_data']['value'] == 100
        assert changes[1]['new_data']['value'] == 200
    
    def test_delete_capture(self, setup_db):
        """Test capturing DELETE operations"""
        cdc = CDCSystem(setup_db, "test_table")
        cdc.setup_trigger_based_cdc(['id', 'name', 'value'])
        
        # Insert and delete data
        setup_db.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", 
                        ("test1", 100))
        setup_db.commit()
        
        setup_db.execute("DELETE FROM test_table WHERE name = 'test1'")
        setup_db.commit()
        
        # Check CDC captured both operations
        changes = cdc.get_pending_changes()
        assert len(changes) == 2
        assert changes[1]['operation'] == 'DELETE'
        assert changes[1]['old_data']['name'] == 'test1'
    
    def test_mark_as_synced(self, setup_db):
        """Test marking changes as synced"""
        cdc = CDCSystem(setup_db, "test_table")
        cdc.setup_trigger_based_cdc(['id', 'name', 'value'])
        
        # Insert data
        setup_db.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", 
                        ("test1", 100))
        setup_db.commit()
        
        # Get and mark as synced
        changes = cdc.get_pending_changes()
        cdc.mark_as_synced([c['cdc_id'] for c in changes])
        
        # Verify no pending changes
        pending = cdc.get_pending_changes()
        assert len(pending) == 0
    
    def test_replication(self, temp_db):
        """Test end-to-end replication"""
        # Setup source
        source_db = SQLiteConnection(temp_db).connect()
        source_db.execute('''
            CREATE TABLE source_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER NOT NULL
            )
        ''')
        source_db.commit()
        
        # Setup target
        target_path = temp_db.replace('.db', '_target.db')
        target_db = SQLiteConnection(target_path).connect()
        target_db.execute('''
            CREATE TABLE target_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER NOT NULL
            )
        ''')
        target_db.commit()
        
        # Setup CDC
        cdc = CDCSystem(source_db, "source_table")
        cdc.setup_trigger_based_cdc(['id', 'name', 'value'])
        
        # Insert data
        source_db.execute("INSERT INTO source_table (name, value) VALUES (?, ?)", 
                         ("test1", 100))
        source_db.commit()
        
        # Replicate
        replicator = CDCReplicator(cdc, target_db, "target_table")
        count = replicator.replicate_changes()
        
        # Verify replication
        assert count == 1
        target_db.execute("SELECT * FROM target_table WHERE name = 'test1'")
        result = target_db.fetchone()
        assert result is not None
        
        # Cleanup
        source_db.close()
        target_db.close()
        os.unlink(target_path)
    
    def test_statistics(self, setup_db):
        """Test CDC statistics"""
        cdc = CDCSystem(setup_db, "test_table")
        cdc.setup_trigger_based_cdc(['id', 'name', 'value'])
        
        # Perform various operations
        setup_db.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", 
                        ("test1", 100))
        setup_db.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", 
                        ("test2", 200))
        setup_db.commit()
        
        setup_db.execute("UPDATE test_table SET value = 150 WHERE name = 'test1'")
        setup_db.commit()
        
        setup_db.execute("DELETE FROM test_table WHERE name = 'test2'")
        setup_db.commit()
        
        # Get statistics
        stats = cdc.get_change_statistics()
        
        assert 'INSERT' in stats
        assert 'UPDATE' in stats
        assert 'DELETE' in stats
        assert stats['INSERT']['total'] == 2
        assert stats['UPDATE']['total'] == 1
        assert stats['DELETE']['total'] == 1


class TestCDCMonitor:
    """Test suite for CDC Monitor"""
    
    @pytest.fixture
    def setup_monitored_cdc(self, tmp_path):
        """Setup CDC with monitoring"""
        db_path = tmp_path / "test.db"
        db = SQLiteConnection(str(db_path)).connect()
        
        db.execute('''
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )
        ''')
        db.commit()
        
        cdc = CDCSystem(db, "test_table")
        cdc.setup_trigger_based_cdc(['id', 'name'])
        
        monitor = CDCMonitor(cdc)
        
        return db, cdc, monitor
    
    def test_health_report(self, tmp_path):
        """Test health report generation"""
        db, cdc, monitor = self.setup_monitored_cdc(tmp_path)
        
        # Insert some data
        db.execute("INSERT INTO test_table (name) VALUES (?)", ("test1",))
        db.commit()
        
        # Get health report
        report = monitor.get_health_report()
        
        assert 'timestamp' in report
        assert 'table' in report
        assert report['table'] == 'test_table'
        assert report['pending_changes'] > 0
        assert 'health_status' in report


# Integration test
def test_full_cdc_workflow():
    """Test complete CDC workflow"""
    import tempfile
    
    # Create temporary databases
    with tempfile.TemporaryDirectory() as tmpdir:
        source_path = os.path.join(tmpdir, "source.db")
        target_path = os.path.join(tmpdir, "target.db")
        
        # Setup source
        source_db = SQLiteConnection(source_path).connect()
        source_db.execute('''
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT DEFAULT 'pending'
            )
        ''')
        source_db.commit()
        
        # Setup target
        target_db = SQLiteConnection(target_path).connect()
        target_db.execute('''
            CREATE TABLE orders_replica (
                id INTEGER PRIMARY KEY,
                customer TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT
            )
        ''')
        target_db.commit()
        
        # Setup CDC
        cdc = CDCSystem(source_db, "orders")
        cdc.setup_trigger_based_cdc(['id', 'customer', 'amount', 'status'])
        
        # Setup replicator
        replicator = CDCReplicator(cdc, target_db, "orders_replica")
        
        # Perform operations
        source_db.execute(
            "INSERT INTO orders (customer, amount) VALUES (?, ?)",
            ("John Doe", 100.50)
        )
        source_db.execute(
            "INSERT INTO orders (customer, amount) VALUES (?, ?)",
            ("Jane Smith", 250.75)
        )
        source_db.commit()
        
        # Replicate
        count = replicator.replicate_changes()
        assert count == 2
        
        # Verify target
        target_db.execute("SELECT COUNT(*) as count FROM orders_replica")
        result = target_db.fetchone()
        assert result['count'] == 2
        
        # Update and replicate
        source_db.execute("UPDATE orders SET status = 'completed' WHERE customer = 'John Doe'")
        source_db.commit()
        
        count = replicator.replicate_changes()
        assert count == 1
        
        # Verify update
        target_db.execute("SELECT status FROM orders_replica WHERE customer = 'John Doe'")
        result = target_db.fetchone()
        assert result['status'] == 'completed'
        
        # Cleanup
        source_db.close()
        target_db.close()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])