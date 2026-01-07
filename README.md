# Change Data Capture (CDC) System for Python

A production-ready, database-agnostic Change Data Capture system that captures and replicates INSERT, UPDATE, and DELETE operations across databases.

##  Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)

##  Features

- **Multiple CDC Strategies**: Trigger-based, timestamp-based, and version-based CDC
- **Database Support**: SQLite, PostgreSQL, MySQL
- **Automatic Replication**: Real-time or batch synchronization
- **Change Tracking**: Captures INSERT, UPDATE, DELETE with before/after values
- **Monitoring**: Built-in health monitoring and statistics
- **Production Ready**: Error handling, logging, and retry mechanisms
- **Extensible**: Easy to add custom database adapters

##  Prerequisites

- Python 3.8 or higher
- SQLite (included with Python)
- PostgreSQL or MySQL (optional, for production use)

##  Installation

### Step 1: Clone or Download the Repository

```bash
# Clone the repository
git clone https://github.com/yourusername/cdc-system.git
cd cdc-system

# Or download the files directly and organize them as:
# cdc-system/
# ├── cdc_system.py
# ├── example_usage.py
# ├── config.py
# ├── requirements.txt
# └── README.md
```

### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

Create `requirements.txt` with:
```
psycopg2-binary==2.9.9
mysql-connector-python==8.2.0
python-dotenv==1.0.0
```

##  Quick Start

Run the basic demo to see CDC in action:

```bash
python example_usage.py basic
```

This will:
1. Create source and target databases
2. Set up CDC infrastructure with triggers
3. Insert, update, and delete sample data
4. Replicate changes to the target database
5. Show statistics and verify replication


## CDC Architecture

```text
┌─────────────────┐
│  Source DB      │
│  (Production)   │
└────────┬────────┘
         │
         │ Triggers capture
         │ INSERT / UPDATE / DELETE
         ↓
┌─────────────────┐
│  CDC Audit      │
│  Table          │
└────────┬────────┘
         │
         │ CDC Replicator
         │ reads changes
         ↓
┌─────────────────┐
│  Target DB      │
│  (Replica)      │
└─────────────────┘
 ```


Methods:
- `setup_trigger_based_cdc(columns)` - Setup CDC infrastructure
- `get_pending_changes(limit)` - Get unsynced changes
- `mark_as_synced(cdc_ids)` - Mark changes as processed
- `get_change_statistics()` - Get CDC statistics

### CDCReplicator

```python
replicator = CDCReplicator(source_cdc, target_db, target_table)
```

Methods:
- `replicate_changes(batch_size)` - Replicate pending changes

### CDCMonitor

```python
monitor = CDCMonitor(cdc_system)
```

Methods:
- `get_health_report()` - Get system health metrics
- `print_report()` - Print formatted report


##  License

MIT License - feel free to use in your projects!







