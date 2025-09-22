from .parser import ASTNode, SelectStmt, InsertStmt, CreateTableStmt, DropTableStmt, BinaryOpExpr, ColumnRef
from .catalog import CatalogManager


class SemanticAnalyzer:
    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog

    def analyze(self, ast: ASTNode):
        if isinstance(ast, SelectStmt):
            return self.analyze_select(ast)
        elif isinstance(ast, InsertStmt):
            return self.analyze_insert(ast)
        elif isinstance(ast, CreateTableStmt):
            return self.analyze_create_table(ast)
        elif isinstance(ast, DropTableStmt):  # 添加DROP TABLE支持
            return self.analyze_drop_table(ast)
        else:
            raise ValueError(f"Unsupported AST node type: {type(ast)}")

    def analyze_select(self, stmt: SelectStmt):
        # 检查表是否存在
        if not self.catalog.table_exists(stmt.table_name):
            raise ValueError(f"Table {stmt.table_name} does not exist")

        schema = self.catalog.get_schema(stmt.table_name)

        # 检查列是否存在
        if stmt.columns != ['*']:
            for column in stmt.columns:
                if column not in schema.column_dict:
                    raise ValueError(f"Column {column} does not exist in table {stmt.table_name}")

        # 检查WHERE条件中的列
        if stmt.where_clause:
            self._validate_expression(stmt.where_clause, schema)

        return stmt

    def analyze_insert(self, stmt: InsertStmt):
        if not self.catalog.table_exists(stmt.table_name):
            raise ValueError(f"Table {stmt.table_name} does not exist")

        schema = self.catalog.get_schema(stmt.table_name)

        if len(stmt.values) != len(schema.columns):
            raise ValueError(f"Expected {len(schema.columns)} values, got {len(stmt.values)}")

        for i, (value, col_def) in enumerate(zip(stmt.values, schema.columns)):
            if not schema.validate_value(col_def['name'], value):
                raise ValueError(f"Invalid value for column {col_def['name']}: {value}")

        return stmt

    def analyze_create_table(self, stmt: CreateTableStmt):
        if self.catalog.table_exists(stmt.table_name):
            raise ValueError(f"Table {stmt.table_name} already exists")

        # 验证列定义
        for col in stmt.columns:
            if col['type'] not in ['INT', 'VARCHAR']:
                raise ValueError(f"Unsupported data type: {col['type']}")

        return stmt

    def analyze_drop_table(self, stmt: DropTableStmt):
        """语义分析DROP TABLE语句"""
        # 检查表是否存在
        if not self.catalog.table_exists(stmt.table_name):
            raise ValueError(f"Table '{stmt.table_name}' does not exist")

        return stmt

    def _validate_expression(self, expr, schema):
        if isinstance(expr, BinaryOpExpr):
            if isinstance(expr.left, ColumnRef):
                if expr.left.name not in schema.column_dict:
                    raise ValueError(f"Column {expr.left.name} does not exist")
            self._validate_expression(expr.right, schema)