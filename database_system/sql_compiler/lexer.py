# sql_compiler/lexer.py
import re
from typing import List, Tuple
from utils.constants import KEYWORDS, OPERATORS


class Token:
    def __init__(self, type: str, value: str, position: int):
        self.type = type
        self.value = value
        self.position = position

    def __repr__(self):
        return f"Token({self.type}, '{self.value}', {self.position})"


class Lexer:
    def __init__(self):
        self.tokens = []
        self.position = 0

    def tokenize(self, sql: str) -> List[Token]:
        """将SQL语句转换为token序列"""
        self.tokens = []
        self.position = 0
        sql = sql.strip()

        # 移除SQL注释
        sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)  # 单行注释
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)  # 多行注释

        token_specification = [
            ('NUMBER', r'\d+(\.\d*)?'),  # 整数或小数
            ('STRING', r"'(?:[^'\\]|\\.)*'"),  # 字符串
            ('ID', r'[a-zA-Z_][a-zA-Z0-9_]*'),  # 标识符
            ('OP', r'[=<>!]=?|<>|\+|-|\*|\/'),  # 操作符
            ('COMMA', r','),
            ('LPAREN', r'\('),
            ('RPAREN', r'\)'),
            ('SEMI', r';'),
            ('WS', r'\s+'),  # 空白字符
        ]

        tok_regex = '|'.join(f'(?P<{pair[0]}>{pair[1]})' for pair in token_specification)

        for mo in re.finditer(tok_regex, sql):
            kind = mo.lastgroup
            value = mo.group()

            if kind == 'WS':
                continue
            elif kind == 'ID' and value.upper() in KEYWORDS:
                kind = 'KEYWORD'
                value = value.upper()
            elif kind == 'STRING':
                value = value[1:-1]  # 去掉引号

            self.tokens.append(Token(kind, value, mo.start()))

        self.tokens.append(Token('EOF', '', len(sql)))
        return self.tokens