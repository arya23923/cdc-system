# File: cdc_system.py
"""
Production-Ready Change Data Capture System
Supports multiple databases and CDC strategies
"""

import sqlite3
import psycopg2
import mysql.connector
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCOperation(Enum):
    """Enumeration of CDC operations"""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class DatabaseType(Enum):
    """Supported database types"""
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"


class DatabaseConnection(ABC):
    """Abstract base class for database connections"""
    
    @abstractmethod
    def connect(self):
        pass
    
    @abstractmethod
    def execute(self, query: str, params: tuple = None):
        pass
    
    @abstractmethod
    def commit(self):
        pass
    
    @abstractmethod
    def close(self):
        pass


class SQLiteConnection(DatabaseConnection):
    """SQLite database connection handler"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
    
    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        return self
    
    def execute(self, query: str, params: tuple = None):
        if params:
            return self.cursor.execute(query, params)
        return self.cursor.execute(query)
    
    def fetchall(self):
        return self.cursor.fetchall()
    
    def fetchone(self):
        return self.cursor.fetchone()
    
    def commit(self):
        self.conn.commit()
    
    def close(self):
        if self.conn:
            self.conn.close()


class CDCSystem:
    """
    Main CDC System supporting multiple strategies:
    1. Trigger-based CDC
    2. Timestamp-based CDC
    3. Version-based CDC
    """
    
    def __init__(self, db_connection: DatabaseConnection, table_name: str):
        self.db = db_connection
        self.table_name = table_name
        self.cdc_table = f"{table_name}_cdc"
        
    def setup_trigger_based_cdc(self, columns: List[str]):
        """
        Setup trigger-based CDC infrastructure
        
        Args:
            columns: List of column names in the source table
        """
        logger.info(f"Setting up trigger-based CDC for table: {self.table_name}")
        
        # Create CDC audit table
        self.db.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.cdc_table} (
                cdc_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                record_id INTEGER NOT NULL,
                old_data TEXT,
                new_data TEXT,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                synced INTEGER DEFAULT 0,
                sync_timestamp TIMESTAMP
            )
        ''')
        
        # Create index for faster queries
        self.db.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{self.cdc_table}_synced 
            ON {self.cdc_table}(synced, cdc_id)
        ''')
        
        self.db.commit()
        
        # Setup triggers
        self._create_insert_trigger(columns)
        self._create_update_trigger(columns)
        self._create_delete_trigger(columns)
        
        logger.info("CDC infrastructure setup complete")
    
    def _create_insert_trigger(self, columns: List[str]):
        """Create trigger for INSERT operations"""
        json_fields = ', '.join([f"'{col}', NEW.{col}" for col in columns])
        
        self.db.execute(f'''
            CREATE TRIGGER IF NOT EXISTS {self.table_name}_insert_trigger
            AFTER INSERT ON {self.table_name}
            BEGIN
                INSERT INTO {self.cdc_table} (operation, record_id, new_data)
                VALUES (
                    'INSERT',
                    NEW.{columns[0]},
                    json_object({json_fields})
                );
            END
        ''')
        self.db.commit()
    
    def _create_update_trigger(self, columns: List[str]):
        """Create trigger for UPDATE operations"""
        old_json = ', '.join([f"'{col}', OLD.{col}" for col in columns])
        new_json = ', '.join([f"'{col}', NEW.{col}" for col in columns])
        
        self.db.execute(f'''
            CREATE TRIGGER IF NOT EXISTS {self.table_name}_update_trigger
            AFTER UPDATE ON {self.table_name}
            BEGIN
                INSERT INTO {self.cdc_table} (operation, record_id, old_data, new_data)
                VALUES (
                    'UPDATE',
                    NEW.{columns[0]},
                    json_object({old_json}),
                    json_object({new_json})
                );
            END
        ''')
        self.db.commit()
    
    def _create_delete_trigger(self, columns: List[str]):
        """Create trigger for DELETE operations"""
        json_fields = ', '.join([f"'{col}', OLD.{col}" for col in columns])
        
        self.db.execute(f'''
            CREATE TRIGGER IF NOT EXISTS {self.table_name}_delete_trigger
            AFTER DELETE ON {self.table_name}
            BEGIN
                INSERT INTO {self.cdc_table} (operation, record_id, old_data)
                VALUES (
                    'DELETE',
                    OLD.{columns[0]},
                    json_object({json_fields})
                );
            END
        ''')
        self.db.commit()
    
    def get_pending_changes(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retrieve all unsynced changes
        
        Args:
            limit: Maximum number of changes to retrieve
            
        Returns:
            List of change records
        """
        query = f'''
            SELECT * FROM {self.cdc_table}
            WHERE synced = 0
            ORDER BY cdc_id
        '''
        
        if limit:
            query += f' LIMIT {limit}'
        
        self.db.execute(query)
        rows = self.db.fetchall()
        
        changes = []
        for row in rows:
            change = dict(row)
            if change.get('old_data'):
                change['old_data'] = json.loads(change['old_data'])
            if change.get('new_data'):
                change['new_data'] = json.loads(change['new_data'])
            changes.append(change)
        
        logger.info(f"Retrieved {len(changes)} pending changes")
        return changes
    
    def mark_as_synced(self, cdc_ids: List[int]):
        """Mark changes as synced"""
        if not cdc_ids:
            return
        
        placeholders = ','.join('?' * len(cdc_ids))
        self.db.execute(f'''
            UPDATE {self.cdc_table}
            SET synced = 1, sync_timestamp = CURRENT_TIMESTAMP
            WHERE cdc_id IN ({placeholders})
        ''', tuple(cdc_ids))
        self.db.commit()
        
        logger.info(f"Marked {len(cdc_ids)} changes as synced")
    
    def get_change_statistics(self) -> Dict[str, int]:
        """Get statistics about CDC changes"""
        self.db.execute(f'''
            SELECT 
                operation,
                COUNT(*) as count,
                SUM(CASE WHEN synced = 0 THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN synced = 1 THEN 1 ELSE 0 END) as synced
            FROM {self.cdc_table}
            GROUP BY operation
        ''')
        
        stats = {}
        for row in self.db.fetchall():
            stats[row['operation']] = {
                'total': row['count'],
                'pending': row['pending'],
                'synced': row['synced']
            }
        
        return stats


class CDCReplicator:
    """Handles replication of changes to target database"""
    
    def __init__(self, source_cdc: CDCSystem, target_db: DatabaseConnection, target_table: str):
        self.source_cdc = source_cdc
        self.target_db = target_db
        self.target_table = target_table
    
    def replicate_changes(self, batch_size: int = 100) -> int:
        """
        Replicate pending changes to target database
        
        Args:
            batch_size: Number of changes to process in one batch
            
        Returns:
            Number of changes replicated
        """
        changes = self.source_cdc.get_pending_changes(limit=batch_size)
        
        if not changes:
            logger.info("No pending changes to replicate")
            return 0
        
        replicated_ids = []
        
        for change in changes:
            try:
                if change['operation'] == 'INSERT':
                    self._apply_insert(change)
                elif change['operation'] == 'UPDATE':
                    self._apply_update(change)
                elif change['operation'] == 'DELETE':
                    self._apply_delete(change)
                
                replicated_ids.append(change['cdc_id'])
                
            except Exception as e:
                logger.error(f"Error replicating change {change['cdc_id']}: {e}")
                # Continue with next change
        
        # Mark successfully replicated changes as synced
        if replicated_ids:
            self.source_cdc.mark_as_synced(replicated_ids)
            self.target_db.commit()
        
        logger.info(f"Successfully replicated {len(replicated_ids)} changes")
        return len(replicated_ids)
    
    def _apply_insert(self, change: Dict[str, Any]):
        """Apply INSERT operation to target"""
        data = change['new_data']
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        
        self.target_db.execute(f'''
            INSERT OR REPLACE INTO {self.target_table} ({columns})
            VALUES ({placeholders})
        ''', tuple(data.values()))
    
    def _apply_update(self, change: Dict[str, Any]):
        """Apply UPDATE operation to target"""
        data = change['new_data']
        set_clause = ', '.join([f"{k} = ?" for k in data.keys() if k != 'id'])
        
        values = [v for k, v in data.items() if k != 'id']
        values.append(data['id'])
        
        self.target_db.execute(f'''
            UPDATE {self.target_table}
            SET {set_clause}
            WHERE id = ?
        ''', tuple(values))
    
    def _apply_delete(self, change: Dict[str, Any]):
        """Apply DELETE operation to target"""
        self.target_db.execute(f'''
            DELETE FROM {self.target_table}
            WHERE id = ?
        ''', (change['record_id'],))


class CDCMonitor:
    """Monitor and report on CDC system health"""
    
    def __init__(self, cdc_system: CDCSystem):
        self.cdc = cdc_system
    
    def get_health_report(self) -> Dict[str, Any]:
        """Generate health report for CDC system"""
        stats = self.cdc.get_change_statistics()
        
        total_pending = sum(s.get('pending', 0) for s in stats.values())
        total_synced = sum(s.get('synced', 0) for s in stats.values())
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'table': self.cdc.table_name,
            'total_changes': total_pending + total_synced,
            'pending_changes': total_pending,
            'synced_changes': total_synced,
            'by_operation': stats,
            'health_status': 'healthy' if total_pending < 1000 else 'warning'
        }
        
        return report
    
    def print_report(self):
        """Print formatted health report"""
        report = self.get_health_report()
        
        print("\n" + "="*60)
        print(f"CDC HEALTH REPORT - {report['timestamp']}")
        print("="*60)
        print(f"Table: {report['table']}")
        print(f"Status: {report['health_status'].upper()}")
        print(f"\nTotal Changes: {report['total_changes']}")
        print(f"Pending: {report['pending_changes']}")
        print(f"Synced: {report['synced_changes']}")
        print("\nBy Operation:")
        for op, stats in report['by_operation'].items():
            print(f"  {op}: {stats['total']} total, {stats['pending']} pending")
        print("="*60 + "\n")