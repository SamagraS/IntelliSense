"""
schema_repository_sqlite.py
============================
SQLite-backed persistence layer for schema_service.py using SQLAlchemy.

This replaces InMemorySchemaRepository with durable storage while maintaining
the same interface contract.

Database Schema
---------------
Table: schema_mappings
    - mapping_id (PK, TEXT): UUID primary key
    - case_id (TEXT, indexed)
    - document_type (TEXT, indexed)
    - schema_template_version (TEXT)
    - schema_fields_json (TEXT): JSON-serialized list of SchemaField objects
    - field_mappings_json (TEXT): JSON dict of raw_column → field_name
    - custom_fields_json (TEXT): JSON list of user-added SchemaField objects
    - manual_edits_json (TEXT): JSON list of ManualEdit objects
    - validated_by (TEXT, nullable)
    - validation_timestamp (DATETIME, nullable)
    - created_at (DATETIME)
    - updated_at (DATETIME)

Unique index on (case_id, document_type) to enforce one active mapping per pair.

Usage
-----
    from schema_repository_sqlite import SQLiteSchemaRepository
    
    repo = SQLiteSchemaRepository(db_path="schema.db")
    record = repo.get_by_case_and_type("CASE_001", "ALM")
"""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import (
    create_engine,
    Column,
    String,
    DateTime,
    Text,
    Index,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.exc import IntegrityError

# Import Pydantic models from schema_service
# These will be imported when schema_service imports this module
# We'll handle the circular import by accepting them as parameters in init
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from schema_service import SchemaMappingRecord, SchemaField

Base = declarative_base()


# ---------------------------------------------------------------------------
# SQLAlchemy ORM Model
# ---------------------------------------------------------------------------

class SchemaMappingORM(Base):
    """
    SQLAlchemy model for schema_mappings table.
    
    Stores SchemaMappingRecord objects serialized to JSON in various columns.
    """
    __tablename__ = "schema_mappings"
    
    mapping_id = Column(String(36), primary_key=True)
    case_id = Column(String(128), nullable=False)
    document_type = Column(String(64), nullable=False)
    schema_template_version = Column(String(32), nullable=False, default="1.0.0")
    
    # JSON-serialized Pydantic model lists/dicts
    schema_fields_json = Column(Text, nullable=False, default="[]")
    field_mappings_json = Column(Text, nullable=False, default="{}")
    custom_fields_json = Column(Text, nullable=False, default="[]")
    manual_edits_json = Column(Text, nullable=False, default="[]")
    
    validated_by = Column(String(128), nullable=True)
    validation_timestamp = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    # Unique constraint on (case_id, document_type)
    __table_args__ = (
        Index("idx_case_document", "case_id", "document_type", unique=True),
    )


# ---------------------------------------------------------------------------
# SQLite Repository Implementation
# ---------------------------------------------------------------------------

class SQLiteSchemaRepository:
    """
    SQLite-backed repository implementing the same interface as
    InMemorySchemaRepository.
    
    Thread-safe when using scoped sessions. Each method creates its own
    session and commits/closes it.
    
    Parameters
    ----------
    db_path : str | Path
        Path to the SQLite database file. Will be created if it doesn't exist.
    echo : bool
        If True, log all SQL statements (useful for debugging).
    """
    
    def __init__(
        self,
        db_path: str | Path = "schema.db",
        echo: bool = False,
    ) -> None:
        self.db_path = Path(db_path)
        self.engine = create_engine(
            f"sqlite:///{self.db_path}",
            echo=echo,
            # Important: SQLite needs check_same_thread=False for FastAPI
            connect_args={"check_same_thread": False},
        )
        
        # Create all tables
        Base.metadata.create_all(self.engine)
        
        # Session factory
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )
    
    # ------------------------------------------------------------------
    # Private helpers for serialization
    # ------------------------------------------------------------------
    
    def _serialize_record(
        self,
        record: "SchemaMappingRecord",
    ) -> SchemaMappingORM:
        """Convert Pydantic SchemaMappingRecord to ORM model."""
        return SchemaMappingORM(
            mapping_id=record.mapping_id,
            case_id=record.case_id,
            document_type=record.document_type,
            schema_template_version=record.schema_template_version,
            schema_fields_json=json.dumps(
                [field.model_dump() for field in record.schema_fields]
            ),
            field_mappings_json=json.dumps(record.field_mappings),
            custom_fields_json=json.dumps(
                [field.model_dump() for field in record.custom_fields_added]
            ),
            manual_edits_json=json.dumps(
                [edit.model_dump(mode="json") for edit in record.manual_edits_applied]
            ),
            validated_by=record.validated_by,
            validation_timestamp=record.validation_timestamp,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
    
    def _deserialize_record(
        self,
        orm_obj: SchemaMappingORM,
        schema_field_class: type,
        manual_edit_class: type,
        mapping_record_class: type,
    ) -> "SchemaMappingRecord":
        """Convert ORM model to Pydantic SchemaMappingRecord."""
        schema_fields = [
            schema_field_class(**field_dict)
            for field_dict in json.loads(orm_obj.schema_fields_json)
        ]
        custom_fields = [
            schema_field_class(**field_dict)
            for field_dict in json.loads(orm_obj.custom_fields_json)
        ]
        manual_edits = [
            manual_edit_class(**edit_dict)
            for edit_dict in json.loads(orm_obj.manual_edits_json)
        ]
        
        return mapping_record_class(
            mapping_id=orm_obj.mapping_id,
            case_id=orm_obj.case_id,
            document_type=orm_obj.document_type,
            schema_template_version=orm_obj.schema_template_version,
            schema_fields=schema_fields,
            field_mappings=json.loads(orm_obj.field_mappings_json),
            custom_fields_added=custom_fields,
            manual_edits_applied=manual_edits,
            validated_by=orm_obj.validated_by,
            validation_timestamp=orm_obj.validation_timestamp,
            created_at=orm_obj.created_at,
            updated_at=orm_obj.updated_at,
        )
    
    # ------------------------------------------------------------------
    # Public API (matches InMemorySchemaRepository interface)
    # ------------------------------------------------------------------
    
    def get_by_case_and_type(
        self,
        case_id: str,
        document_type: str,
    ) -> Optional["SchemaMappingRecord"]:
        """
        Retrieve mapping record by (case_id, document_type) pair.
        
        Returns None if no record exists.
        """
        # Import here to avoid circular dependency
        from schema_service import SchemaMappingRecord, SchemaField, ManualEdit
        
        session: Session = self.SessionLocal()
        try:
            orm_obj = (
                session.query(SchemaMappingORM)
                .filter_by(case_id=case_id, document_type=document_type)
                .first()
            )
            
            if orm_obj is None:
                return None
            
            return self._deserialize_record(
                orm_obj,
                SchemaField,
                ManualEdit,
                SchemaMappingRecord,
            )
        finally:
            session.close()
    
    def get_by_mapping_id(
        self,
        mapping_id: str,
    ) -> Optional["SchemaMappingRecord"]:
        """
        Retrieve mapping record by its primary key (mapping_id).
        
        Returns None if no record exists.
        """
        from schema_service import SchemaMappingRecord, SchemaField, ManualEdit
        
        session: Session = self.SessionLocal()
        try:
            orm_obj = (
                session.query(SchemaMappingORM)
                .filter_by(mapping_id=mapping_id)
                .first()
            )
            
            if orm_obj is None:
                return None
            
            return self._deserialize_record(
                orm_obj,
                SchemaField,
                ManualEdit,
                SchemaMappingRecord,
            )
        finally:
            session.close()
    
    def upsert(
        self,
        record: "SchemaMappingRecord",
    ) -> "SchemaMappingRecord":
        """
        Insert or update the mapping record.
        
        Updates updated_at timestamp automatically.
        If a record with the same (case_id, document_type) exists but different
        mapping_id, the old one is deleted first (enforces unique constraint).
        """
        record.updated_at = datetime.now(timezone.utc)
        
        session: Session = self.SessionLocal()
        try:
            # Check if a record exists with same (case_id, document_type)
            existing = (
                session.query(SchemaMappingORM)
                .filter_by(
                    case_id=record.case_id,
                    document_type=record.document_type,
                )
                .first()
            )
            
            if existing:
                # Delete old record if mapping_id differs
                if existing.mapping_id != record.mapping_id:
                    session.delete(existing)
                    session.commit()
                else:
                    # Update existing record
                    session.delete(existing)
                    session.commit()
            
            # Insert new/updated record
            orm_obj = self._serialize_record(record)
            session.add(orm_obj)
            session.commit()
            session.refresh(orm_obj)
            
            return record
        except IntegrityError as e:
            session.rollback()
            raise RuntimeError(
                f"Database constraint violation for case_id={record.case_id}, "
                f"document_type={record.document_type}: {e}"
            )
        finally:
            session.close()
    
    def create_for_case(
        self,
        case_id: str,
        document_type: str,
    ) -> "SchemaMappingRecord":
        """
        Bootstrap a new empty record with the base template pre-loaded.
        
        This method needs access to SCHEMA_TEMPLATES and TEMPLATE_VERSION
        from schema_service, so it imports them dynamically.
        """
        from schema_service import (
            SchemaMappingRecord,
            SCHEMA_TEMPLATES,
            TEMPLATE_VERSION,
        )
        
        # Deep copy template fields to avoid mutation
        fields = deepcopy(SCHEMA_TEMPLATES.get(document_type, []))
        
        record = SchemaMappingRecord(
            case_id=case_id,
            document_type=document_type,
            schema_fields=fields,
            schema_template_version=TEMPLATE_VERSION,
        )
        
        return self.upsert(record)
    
    # ------------------------------------------------------------------
    # Additional utility methods
    # ------------------------------------------------------------------
    
    def delete_by_mapping_id(self, mapping_id: str) -> bool:
        """
        Delete a record by mapping_id.
        
        Returns True if deleted, False if not found.
        """
        session: Session = self.SessionLocal()
        try:
            deleted = (
                session.query(SchemaMappingORM)
                .filter_by(mapping_id=mapping_id)
                .delete()
            )
            session.commit()
            return deleted > 0
        finally:
            session.close()
    
    def list_all_mappings(self) -> list["SchemaMappingRecord"]:
        """
        Retrieve all mapping records (for admin/debugging).
        
        Use with caution in production with large datasets.
        """
        from schema_service import SchemaMappingRecord, SchemaField, ManualEdit
        
        session: Session = self.SessionLocal()
        try:
            orm_objects = session.query(SchemaMappingORM).all()
            return [
                self._deserialize_record(
                    obj,
                    SchemaField,
                    ManualEdit,
                    SchemaMappingRecord,
                )
                for obj in orm_objects
            ]
        finally:
            session.close()
    
    def count_mappings(self) -> int:
        """Return total count of mapping records."""
        session: Session = self.SessionLocal()
        try:
            return session.query(SchemaMappingORM).count()
        finally:
            session.close()


# ---------------------------------------------------------------------------
# Factory function for easy instantiation
# ---------------------------------------------------------------------------

def create_sqlite_repository(
    db_path: str | Path = "schema.db",
    echo: bool = False,
) -> SQLiteSchemaRepository:
    """
    Factory function to create a SQLite repository instance.
    
    Parameters
    ----------
    db_path : str | Path
        Path to SQLite database file. Default: "schema.db"
    echo : bool
        If True, log SQL statements. Default: False
    
    Returns
    -------
    SQLiteSchemaRepository
        Configured repository instance with initialized database.
    """
    return SQLiteSchemaRepository(db_path=db_path, echo=echo)


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Example: Create a repository and test basic operations
    print("Initializing SQLite repository...")
    repo = create_sqlite_repository(db_path="test_schema.db", echo=True)
    
    print(f"\nDatabase created at: {repo.db_path.absolute()}")
    print(f"Total mappings: {repo.count_mappings()}")
    
    # The actual usage requires SchemaMappingRecord from schema_service
    # This is just a demonstration of the repository initialization
    print("\n✓ SQLite schema repository ready!")
