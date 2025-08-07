"""DuckDB SQL generation for tables, indexes, and constraints."""

import logging
from typing import Dict, List
from portfolio_manager.infrastructure.data_access.schema_manager import TableDefinition
from .schema_definitions import IndexDefinition

logger = logging.getLogger(__name__)


class DuckDBTableBuilder:
    """SQL generation for DuckDB tables, indexes, and constraints.
    
    Handles DuckDB-specific SQL generation with optimizations for
    columnar storage and analytical workloads.
    """
    
    def build_create_table_sql(self, table_def: TableDefinition) -> str:
        """Generate CREATE TABLE SQL for DuckDB.
        
        Args:
            table_def: Table definition to generate SQL for
            
        Returns:
            Complete CREATE TABLE SQL statement
        """
        sql_parts = []
        sql_parts.append(f"CREATE TABLE IF NOT EXISTS {table_def.name} (")
        
        # Add column definitions
        column_lines = []
        for column_name, column_def in table_def.columns.items():
            column_lines.append(f"    {column_name} {column_def}")
        
        # Add primary key constraint (only if not already specified in column definition)
        if table_def.primary_key:
            # Check if any column already has PRIMARY KEY in its definition
            has_inline_pk = any("PRIMARY KEY" in col_def for col_def in table_def.columns.values())
            
            if not has_inline_pk:
                if len(table_def.primary_key) == 1:
                    pk_constraint = f"    PRIMARY KEY ({table_def.primary_key[0]})"
                else:
                    pk_columns = ", ".join(table_def.primary_key)
                    pk_constraint = f"    PRIMARY KEY ({pk_columns})"
                column_lines.append(pk_constraint)
        
        # Add table-level constraints
        if table_def.constraints:
            for constraint in table_def.constraints:
                column_lines.append(f"    {constraint}")
        
        sql_parts.append(",\n".join(column_lines))
        sql_parts.append(");")
        
        return "\n".join(sql_parts)
    
    def build_create_index_sql(self, index_def: IndexDefinition) -> str:
        """Generate CREATE INDEX SQL for DuckDB.
        
        Args:
            index_def: Index definition to generate SQL for
            
        Returns:
            CREATE INDEX SQL statement
        """
        unique_clause = "UNIQUE " if index_def.unique else ""
        columns_clause = ", ".join(index_def.columns)
        where_clause = f" WHERE {index_def.where_clause}" if index_def.where_clause else ""
        
        return (
            f"CREATE {unique_clause}INDEX {index_def.name} "
            f"ON {index_def.table} ({columns_clause}){where_clause};"
        )
    
    def build_drop_table_sql(self, table_name: str, cascade: bool = False) -> str:
        """Generate DROP TABLE SQL.
        
        Args:
            table_name: Name of table to drop
            cascade: Whether to cascade drop to dependent objects
            
        Returns:
            DROP TABLE SQL statement
        """
        cascade_clause = " CASCADE" if cascade else ""
        return f"DROP TABLE IF EXISTS {table_name}{cascade_clause};"
    
    def build_drop_index_sql(self, index_name: str) -> str:
        """Generate DROP INDEX SQL.
        
        Args:
            index_name: Name of index to drop
            
        Returns:
            DROP INDEX SQL statement
        """
        return f"DROP INDEX IF EXISTS {index_name};"
    
    def build_add_foreign_key_sql(self, table_name: str, column: str, reference: str) -> str:
        """Generate ALTER TABLE ADD FOREIGN KEY SQL.
        
        Args:
            table_name: Table to add foreign key to
            column: Column name for foreign key
            reference: Referenced table.column
            
        Returns:
            ALTER TABLE SQL statement
        """
        fk_name = f"fk_{table_name}_{column}"
        return (
            f"ALTER TABLE {table_name} "
            f"ADD CONSTRAINT {fk_name} "
            f"FOREIGN KEY ({column}) REFERENCES {reference};"
        )
    
    def build_add_check_constraint_sql(self, table_name: str, constraint_name: str, condition: str) -> str:
        """Generate ALTER TABLE ADD CHECK constraint SQL.
        
        Args:
            table_name: Table to add constraint to
            constraint_name: Name for the constraint
            condition: Check condition
            
        Returns:
            ALTER TABLE SQL statement
        """
        return (
            f"ALTER TABLE {table_name} "
            f"ADD CONSTRAINT {constraint_name} "
            f"CHECK ({condition});"
        )
    
    def build_create_view_sql(self, view_name: str, sql: str) -> str:
        """Generate CREATE VIEW SQL.
        
        Args:
            view_name: Name of view to create
            sql: View definition SQL
            
        Returns:
            CREATE VIEW SQL statement
        """
        return f"CREATE VIEW {view_name} AS\n{sql};"
    
    def build_drop_view_sql(self, view_name: str) -> str:
        """Generate DROP VIEW SQL.
        
        Args:
            view_name: Name of view to drop
            
        Returns:
            DROP VIEW SQL statement
        """
        return f"DROP VIEW IF EXISTS {view_name};"
    
    def get_table_creation_order(self, tables: Dict[str, TableDefinition]) -> List[str]:
        """Determine optimal table creation order based on foreign key dependencies.
        
        Args:
            tables: Dictionary of table definitions
            
        Returns:
            List of table names in dependency order
        """
        # Build dependency graph
        dependencies = {}
        for table_name, table_def in tables.items():
            deps = set()
            for column, reference in table_def.foreign_keys.items():
                if "." in reference:
                    referenced_table = reference.split(".")[0]
                    if referenced_table != table_name and referenced_table in tables:
                        deps.add(referenced_table)
            dependencies[table_name] = deps
        
        # Topological sort
        ordered = []
        visited = set()
        visiting = set()
        
        def visit(table: str):
            if table in visiting:
                # Circular dependency - log warning and continue
                logger.warning(f"Circular dependency detected involving table: {table}")
                return
            if table in visited:
                return
                
            visiting.add(table)
            for dep in dependencies.get(table, set()):
                visit(dep)
            visiting.remove(table)
            visited.add(table)
            ordered.append(table)
        
        for table_name in tables.keys():
            visit(table_name)
        
        return ordered
    
    def get_table_drop_order(self, tables: Dict[str, TableDefinition]) -> List[str]:
        """Determine optimal table drop order (reverse of creation order).
        
        Args:
            tables: Dictionary of table definitions
            
        Returns:
            List of table names in reverse dependency order
        """
        creation_order = self.get_table_creation_order(tables)
        return list(reversed(creation_order))
    
    def build_complete_schema_sql(self, tables: Dict[str, TableDefinition], indexes: List[IndexDefinition]) -> str:
        """Generate complete schema creation SQL.
        
        Args:
            tables: All table definitions
            indexes: All index definitions
            
        Returns:
            Complete SQL script to create entire schema
        """
        sql_parts = []
        sql_parts.append("-- Portfolio Manager Database Schema")
        sql_parts.append("-- Generated by DuckDBTableBuilder")
        sql_parts.append("")
        
        # Create tables in dependency order
        sql_parts.append("-- Create Tables")
        ordered_tables = self.get_table_creation_order(tables)
        
        for table_name in ordered_tables:
            if table_name in tables:
                table_def = tables[table_name]
                sql_parts.append(f"-- Table: {table_name}")
                sql_parts.append(self.build_create_table_sql(table_def))
                sql_parts.append("")
        
        # Add foreign key constraints (after all tables are created)
        sql_parts.append("-- Add Foreign Key Constraints")
        for table_name in ordered_tables:
            if table_name in tables:
                table_def = tables[table_name]
                for column, reference in table_def.foreign_keys.items():
                    sql_parts.append(self.build_add_foreign_key_sql(table_name, column, reference))
        
        if any(table_def.foreign_keys for table_def in tables.values()):
            sql_parts.append("")
        
        # Create indexes
        if indexes:
            sql_parts.append("-- Create Indexes")
            for index_def in indexes:
                sql_parts.append(f"-- Index: {index_def.name}")
                sql_parts.append(self.build_create_index_sql(index_def))
            sql_parts.append("")
        
        return "\n".join(sql_parts)
    
    def build_complete_drop_schema_sql(self, tables: Dict[str, TableDefinition], indexes: List[IndexDefinition]) -> str:
        """Generate complete schema drop SQL.
        
        Args:
            tables: All table definitions
            indexes: All index definitions
            
        Returns:
            Complete SQL script to drop entire schema
        """
        sql_parts = []
        sql_parts.append("-- Drop Portfolio Manager Database Schema")
        sql_parts.append("-- Generated by DuckDBTableBuilder")
        sql_parts.append("")
        
        # Drop indexes first
        if indexes:
            sql_parts.append("-- Drop Indexes")
            for index_def in indexes:
                sql_parts.append(self.build_drop_index_sql(index_def.name))
            sql_parts.append("")
        
        # Drop tables in reverse dependency order
        sql_parts.append("-- Drop Tables")
        drop_order = self.get_table_drop_order(tables)
        
        for table_name in drop_order:
            sql_parts.append(self.build_drop_table_sql(table_name, cascade=True))
        
        return "\n".join(sql_parts)
    
    @staticmethod
    def optimize_for_analytics() -> str:
        """Generate DuckDB-specific optimization settings for analytical workloads.
        
        Returns:
            SQL statements to optimize DuckDB for analytics
        """
        return """
-- DuckDB Analytics Optimizations
SET enable_optimizer = true;
SET enable_profiling = false;
SET memory_limit = '4GB';
SET threads = 4;
SET temp_directory = '/tmp/duckdb_temp/';

-- Enable parallel processing
SET enable_progress_bar = false;
SET preserve_insertion_order = false;

-- Optimize for columnar storage
SET default_order = 'ASC';
"""
