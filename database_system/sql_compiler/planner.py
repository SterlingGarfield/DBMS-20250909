from typing import Dict, Any
from .parser import ASTNode, SelectStmt, InsertStmt, CreateTableStmt, DropTableStmt
from .catalog import CatalogManager


class QueryPlan:
    def __init__(self, plan_type: str, details: Dict[str, Any] = None):
        self.plan_type = plan_type
        self.details = details or {}

    def __repr__(self):
        return f"QueryPlan({self.plan_type}, {self.details})"


class Planner:
    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog

    def create_plan(self, ast: ASTNode) -> QueryPlan:
        if isinstance(ast, SelectStmt):
            return self._create_select_plan(ast)
        elif isinstance(ast, InsertStmt):
            return self._create_insert_plan(ast)
        elif isinstance(ast, CreateTableStmt):
            return self._create_create_table_plan(ast)
        elif isinstance(ast, DropTableStmt):  # 添加DROP TABLE支持
            return self._create_drop_table_plan(ast)
        else:
            raise ValueError(f"Unsupported AST node type: {type(ast)}")

    def _create_select_plan(self, stmt: SelectStmt) -> QueryPlan:
        schema = self.catalog.get_schema(stmt.table_name)
        plan_details = {
            'table_name': stmt.table_name,
            'columns': stmt.columns,
            'where_clause': stmt.where_clause,
            'schema': schema
        }
        return QueryPlan('SELECT', plan_details)

    def _create_insert_plan(self, stmt: InsertStmt) -> QueryPlan:
        schema = self.catalog.get_schema(stmt.table_name)
        plan_details = {
            'table_name': stmt.table_name,
            'values': stmt.values,
            'schema': schema
        }
        return QueryPlan('INSERT', plan_details)

    def _create_create_table_plan(self, stmt: CreateTableStmt) -> QueryPlan:
        plan_details = {
            'table_name': stmt.table_name,
            'columns': stmt.columns,
            'primary_key': stmt.primary_key
        }
        return QueryPlan('CREATE_TABLE', plan_details)

    def _create_drop_table_plan(self, stmt: DropTableStmt) -> QueryPlan:
        """生成DROP TABLE执行计划"""
        plan_details = {
            'table_name': stmt.table_name
        }
        return QueryPlan('DROP_TABLE', plan_details)