# sql_compiler/parser.py
from typing import List, Dict, Any
from .lexer import Token, Lexer
from .catalog import CatalogManager


class ASTNode:
    pass


class SelectStmt(ASTNode):
    def __init__(self, columns: List[str], table_name: str, where_clause=None):
        self.columns = columns
        self.table_name = table_name
        self.where_clause = where_clause

class DropTableStmt(ASTNode):
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.type = 'DROP_TABLE'

class InsertStmt(ASTNode):
    def __init__(self, table_name: str, values: List[Any]):
        self.table_name = table_name
        self.values = values


class CreateTableStmt(ASTNode):
    def __init__(self, table_name: str, columns: List[Dict], primary_key: str = None):
        self.table_name = table_name
        self.columns = columns
        self.primary_key = primary_key


class Expr(ASTNode):
    pass


class BinaryOpExpr(Expr):
    def __init__(self, left: Expr, op: str, right: Expr):
        self.left = left
        self.op = op
        self.right = right


class ColumnRef(Expr):
    def __init__(self, name: str):
        self.name = name


class Constant(Expr):
    def __init__(self, value: Any, type: str = None):
        self.value = value
        self.type = type


class Parser:
    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog
        self.tokens: List[Token] = []
        self.pos = 0

    def current_token(self) -> Token:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else self.tokens[-1]

    def eat(self, expected_type: str, expected_value: str = None):
        token = self.current_token()
        if token.type != expected_type:
            raise SyntaxError(f"Expected {expected_type}, got {token.type}")
        if expected_value and token.value != expected_value:
            raise SyntaxError(f"Expected {expected_value}, got {token.value}")
        self.pos += 1
        return token

    def parse_drop_table(self) -> DropTableStmt:
        """解析DROP TABLE语句"""
        self.eat('KEYWORD', 'DROP')
        self.eat('KEYWORD', 'TABLE')

        table_name = self.current_token().value
        self.eat('ID')

        if self.current_token().type == 'SEMI':
            self.eat('SEMI')

        return DropTableStmt(table_name)

    def parse(self, sql: str) -> ASTNode:
        lexer = Lexer()
        self.tokens = lexer.tokenize(sql)
        self.pos = 0

        token = self.current_token()
        if token.type == 'KEYWORD':
            if token.value == 'SELECT':
                return self.parse_select()
            elif token.value == 'INSERT':
                return self.parse_insert()
            elif token.value == 'CREATE':
                return self.parse_create_table()
            elif token.value == 'DROP':  # 添加DROP语句解析
                return self.parse_drop_table()

        raise SyntaxError(f"Unexpected token: {token.value}")

    def parse_select(self) -> SelectStmt:
        self.eat('KEYWORD', 'SELECT')

        # 解析列列表
        columns = []
        if self.current_token().value == '*':
            self.eat('OP')  # 吃掉 * 符号
            columns = ['*']
        else:
            while True:
                # 检查当前token是否为ID类型
                if self.current_token().type != 'ID':
                    raise SyntaxError(f"Expected column name, got {self.current_token().type}")
                self.eat('ID')
                columns.append(self.tokens[self.pos - 1].value)
                if self.current_token().type != 'COMMA':
                    break
                self.eat('COMMA')

        # 检查是否有FROM关键字
        if self.current_token().value != 'FROM':
            raise SyntaxError(f"Expected FROM, got {self.current_token().value}")

        self.eat('KEYWORD', 'FROM')

        # 检查表名是否为ID类型
        if self.current_token().type != 'ID':
            raise SyntaxError(f"Expected table name, got {self.current_token().type}")

        table_name = self.current_token().value
        self.eat('ID')

        where_clause = None
        if self.current_token().value == 'WHERE':
            self.eat('KEYWORD', 'WHERE')
            where_clause = self.parse_condition()

        if self.current_token().type == 'SEMI':
            self.eat('SEMI')

        return SelectStmt(columns, table_name, where_clause)

    def parse_insert(self) -> InsertStmt:
        self.eat('KEYWORD', 'INSERT')
        self.eat('KEYWORD', 'INTO')

        table_name = self.current_token().value
        self.eat('ID')

        self.eat('KEYWORD', 'VALUES')
        self.eat('LPAREN')

        values = []
        while True:
            token = self.current_token()
            if token.type == 'NUMBER':
                value = float(token.value) if '.' in token.value else int(token.value)
                values.append(value)
                self.eat('NUMBER')
            elif token.type == 'STRING':
                values.append(token.value)
                self.eat('STRING')
            elif token.type == 'ID':
                # 处理标识符类型的值（可能是NULL、TRUE、FALSE等）
                value = token.value.upper()
                if value == 'NULL':
                    values.append(None)
                elif value == 'TRUE':
                    values.append(True)
                elif value == 'FALSE':
                    values.append(False)
                else:
                    # 如果是其他标识符，可能是列名引用或未加引号的字符串
                    values.append(token.value)
                self.eat('ID')
            else:
                raise SyntaxError(f"Unexpected value type: {token.type}")

            if self.current_token().type != 'COMMA':
                break
            self.eat('COMMA')

        self.eat('RPAREN')

        if self.current_token().type == 'SEMI':
            self.eat('SEMI')

        return InsertStmt(table_name, values)

    def parse_create_table(self) -> CreateTableStmt:
        self.eat('KEYWORD', 'CREATE')
        self.eat('KEYWORD', 'TABLE')

        table_name = self.current_token().value
        self.eat('ID')
        self.eat('LPAREN')

        columns = []
        primary_key = None

        while True:
            col_name = self.current_token().value
            self.eat('ID')

            type_token = self.current_token()
            if type_token.type == 'KEYWORD' and type_token.value in ['INT', 'VARCHAR']:
                self.eat('KEYWORD')
                col_type = type_token.value
                col_length = None

                if col_type == 'VARCHAR':
                    self.eat('LPAREN')
                    length_token = self.current_token()
                    self.eat('NUMBER')
                    col_length = int(length_token.value)
                    self.eat('RPAREN')

                column_def = {'name': col_name, 'type': col_type, 'length': col_length}

                # 检查是否有PRIMARY KEY
                if self.current_token().value == 'PRIMARY':
                    self.eat('KEYWORD', 'PRIMARY')
                    self.eat('KEYWORD', 'KEY')
                    primary_key = col_name

                columns.append(column_def)

            if self.current_token().type != 'COMMA':
                break
            self.eat('COMMA')

        self.eat('RPAREN')
        if self.current_token().type == 'SEMI':
            self.eat('SEMI')

        return CreateTableStmt(table_name, columns, primary_key)

    def parse_condition(self) -> Expr:
        left = ColumnRef(self.current_token().value)
        self.eat('ID')

        op = self.current_token().value
        self.eat('OP')

        right_token = self.current_token()
        if right_token.type == 'NUMBER':
            value = float(right_token.value) if '.' in right_token.value else int(right_token.value)
            right = Constant(value, 'NUMBER')
            self.eat('NUMBER')
        elif right_token.type == 'STRING':
            right = Constant(right_token.value, 'STRING')
            self.eat('STRING')
        else:
            right = ColumnRef(right_token.value)
            self.eat('ID')

        return BinaryOpExpr(left, op, right)