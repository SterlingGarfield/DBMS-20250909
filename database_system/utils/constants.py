# utils/constants.py
# 页面大小常量
PAGE_SIZE = 4096  # 4KB
RECORD_SIZE = 128  # 每条记录128字节

# 数据类型
INT_TYPE = 'INT'
STRING_TYPE = 'STRING'
FLOAT_TYPE = 'FLOAT'
BOOL_TYPE = 'BOOL'

# SQL关键字 - 添加DROP关键字
KEYWORDS = {
    'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'CREATE', 'TABLE',
    'INT', 'VARCHAR', 'PRIMARY', 'KEY', 'AND', 'OR', 'NOT', 'NULL', 'DROP'
}

# 操作符
OPERATORS = {'=', '>', '<', '>=', '<=', '<>', '!=', 'LIKE'}

# 系统表前缀
SYS_TABLE_PREFIX = 'sys_'