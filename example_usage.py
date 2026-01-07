# File: example_usage.py
"""
Example usage and demo of the CDC system
"""

from cdc_system import (
    CDCSystem, 
    CDCReplicator, 
    CDCMonitor,
    SQLiteConnection
)
import time
import random


def setup_demo_tables(db):
    """Create demo tables for testing"""
    # Create source table
    db.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create target table (replica)
    db.execute('''
        CREATE TABLE IF NOT EXISTS users_replica (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP
        )
    ''')
    
    db.commit()
    print("✓ Demo tables created")


def insert_sample_data(db, count=5):
    """Insert sample data into source table"""
    print(f"\n Inserting {count} sample records...")
    
    for i in range(count):
        username = f"user_{random.randint(1000, 9999)}"
        email = f"{username}@example.com"
        
        db.execute('''
            INSERT INTO users (username, email)
            VALUES (?, ?)
        ''', (username, email))
    
    db.commit()
    print(f"✓ Inserted {count} records")


def update_sample_data(db, count=3):
    """Update some existing records"""
    print(f"\n  Updating {count} records...")
    
    db.execute('SELECT id FROM users LIMIT ?', (count,))
    rows = db.fetchall()
    
    for row in rows:
        new_status = random.choice(['active', 'inactive', 'suspended'])
        db.execute('''
            UPDATE users 
            SET status = ?
            WHERE id = ?
        ''', (new_status, row['id']))
    
    db.commit()
    print(f"✓ Updated {count} records")


def delete_sample_data(db, count=2):
    """Delete some records"""
    print(f"\n  Deleting {count} records...")
    
    db.execute('SELECT id FROM users LIMIT ?', (count,))
    rows = db.fetchall()
    
    for row in rows:
        db.execute('DELETE FROM users WHERE id = ?', (row['id'],))
    
    db.commit()
    print(f"✓ Deleted {count} records")


def verify_replication(source_db, target_db):
    """Verify data consistency between source and target"""
    print("\n Verifying replication...")
    
    # Count records
    source_db.execute('SELECT COUNT(*) as count FROM users')
    source_count = source_db.fetchone()['count']
    
    target_db.execute('SELECT COUNT(*) as count FROM users_replica')
    target_count = target_db.fetchone()['count']
    
    print(f"Source records: {source_count}")
    print(f"Target records: {target_count}")
    
    if source_count == target_count:
        print("✓ Record counts match!")
    else:
        print("⚠ Record count mismatch!")


def run_basic_demo():
    """Run basic CDC demonstration"""
    print("\n" + "="*60)
    print("BASIC CDC DEMONSTRATION")
    print("="*60)
    
    # Setup connections
    source_db = SQLiteConnection("source.db").connect()
    target_db = SQLiteConnection("target.db").connect()
    
    # Setup tables
    setup_demo_tables(source_db)
    setup_demo_tables(target_db)
    
    # Initialize CDC system
    print("\n Initializing CDC system...")
    cdc = CDCSystem(source_db, "users")
    cdc.setup_trigger_based_cdc(['id', 'username', 'email', 'status', 'created_at'])
    print("✓ CDC system initialized")
    
    # Initialize replicator
    replicator = CDCReplicator(cdc, target_db, "users_replica")
    
    # Initialize monitor
    monitor = CDCMonitor(cdc)
    
    # Perform operations
    insert_sample_data(source_db, count=10)
    update_sample_data(source_db, count=5)
    delete_sample_data(source_db, count=2)
    
    # Show CDC statistics
    print("\nCDC Statistics:")
    monitor.print_report()
    
    # Replicate changes
    print("\n Starting replication...")
    replicated = replicator.replicate_changes(batch_size=50)
    print(f"✓ Replicated {replicated} changes")
    
    # Verify replication
    verify_replication(source_db, target_db)
    
    # Show updated statistics
    print("\n Updated CDC Statistics:")
    monitor.print_report()
    
    # Cleanup
    source_db.close()
    target_db.close()
    
    print("\n Demo completed successfully!")


def run_continuous_sync_demo(duration_seconds=30):
    """
    Run continuous synchronization demo
    Simulates real-time CDC with periodic syncing
    """
    print("\n" + "="*60)
    print("CONTINUOUS SYNC DEMONSTRATION")
    print(f"Running for {duration_seconds} seconds...")
    print("="*60)
    
    # Setup
    source_db = SQLiteConnection("source_continuous.db").connect()
    target_db = SQLiteConnection("target_continuous.db").connect()
    
    setup_demo_tables(source_db)
    setup_demo_tables(target_db)
    
    cdc = CDCSystem(source_db, "users")
    cdc.setup_trigger_based_cdc(['id', 'username', 'email', 'status', 'created_at'])
    
    replicator = CDCReplicator(cdc, target_db, "users_replica")
    monitor = CDCMonitor(cdc)
    
    start_time = time.time()
    sync_interval = 5  # Sync every 5 seconds
    last_sync = start_time
    
    print("\n Starting continuous sync...")
    print("(Performing random operations and syncing periodically)\n")
    
    while time.time() - start_time < duration_seconds:
        current_time = time.time()
        
        # Perform random operations
        operation = random.choice(['insert', 'update', 'delete', 'none'])
        
        if operation == 'insert':
            insert_sample_data(source_db, count=1)
        elif operation == 'update':
            update_sample_data(source_db, count=1)
        elif operation == 'delete' and random.random() > 0.7:  # Less frequent deletes
            delete_sample_data(source_db, count=1)
        
        # Sync periodically
        if current_time - last_sync >= sync_interval:
            replicated = replicator.replicate_changes(batch_size=10)
            if replicated > 0:
                print(f"  [{int(current_time - start_time)}s] Synced {replicated} changes")
            last_sync = current_time
        
        time.sleep(1)
    
    # Final sync
    print("\n Performing final sync...")
    final_count = replicator.replicate_changes(batch_size=100)
    print(f"✓ Final sync: {final_count} changes")
    
    # Final report
    print("\n Final Report:")
    monitor.print_report()
    verify_replication(source_db, target_db)
    
    source_db.close()
    target_db.close()
    
    print("\n Continuous sync demo completed!")


def run_conflict_resolution_demo():
    """Demonstrate handling of conflicts in CDC"""
    print("\n" + "="*60)
    print("CONFLICT RESOLUTION DEMONSTRATION")
    print("="*60)
    
    # This would include examples of:
    # - Handling duplicate keys
    # - Last-write-wins strategy
    # - Custom conflict resolution
    
    print("\n Conflict resolution strategies:")
    print("1. Last Write Wins - Most recent change takes precedence")
    print("2. First Write Wins - Original change is preserved")
    print("3. Manual Resolution - Flag conflicts for human review")
    print("4. Merge Strategy - Combine changes intelligently")
    print("\n(Full implementation would go here)")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        demo_type = sys.argv[1]
    else:
        demo_type = "basic"
    
    if demo_type == "basic":
        run_basic_demo()
    elif demo_type == "continuous":
        duration = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        run_continuous_sync_demo(duration)
    elif demo_type == "conflict":
        run_conflict_resolution_demo()
    else:
        print("Usage:")
        print("  python example_usage.py basic          - Run basic demo")
        print("  python example_usage.py continuous [s] - Run continuous sync for s seconds")
        print("  python example_usage.py conflict       - Show conflict resolution patterns")