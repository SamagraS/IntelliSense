"""
init_schema_db.py
=================
Initialize the schema.db database with proper tables.

Run this once to create the database file.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from schema_repository_sqlite import create_sqlite_repository

def initialize_database():
    """Create schema.db with all necessary tables."""
    
    db_path = Path(__file__).parent / "schema.db"
    
    print("=" * 70)
    print("Initializing Schema Database")
    print("=" * 70)
    print(f"\nDatabase location: {db_path.absolute()}")
    
    # Create repository (this automatically creates tables)
    repo = create_sqlite_repository(db_path=db_path, echo=False)
    
    print(f"\n✓ Database created successfully!")
    print(f"✓ Tables created:")
    print(f"  - schema_mappings (with indexes)")
    
    # Verify database exists
    if db_path.exists():
        print(f"\n✓ File exists: {db_path.name}")
        print(f"✓ File size: {db_path.stat().st_size} bytes")
    
    # Show initial state
    count = repo.count_mappings()
    print(f"\n✓ Initial record count: {count}")
    
    print("\n" + "=" * 70)
    print("Database is ready for use!")
    print("=" * 70)
    print("\nYou can now start the schema service:")
    print("  python schema_service.py")
    print("\nOr connect directly with sqlite3:")
    print(f"  sqlite3 {db_path.name}")
    print("=" * 70)
    
    return db_path


if __name__ == "__main__":
    try:
        initialize_database()
    except Exception as e:
        print(f"\n❌ Failed to initialize database: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
