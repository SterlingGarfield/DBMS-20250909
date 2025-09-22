import json
import os
from typing import Dict, List, Optional
from utils.constants import *


class Schema:
    def __init__(self, table_name: str, columns: List[Dict], primary_key: str = None):
        self.table_name = table_name
        self.columns = columns
        self.primary_key = primary_key
        self.column_dict = {col['name']: col for col in columns}

    def get_column_index(self, column_name: str) -> int:
        for i, col in enumerate(self.columns):
            if col['name'] == column_name:
                return i
        return -1

    def validate_value(self, column_name: str, value) -> bool:
        col = self.column_dict.get(column_name)
        if not col:
            return False

        if value is None and not col.get('nullable', True):
            return False

        if col['type'] == INT_TYPE and not isinstance(value, int):
            return False
        elif col['type'] == STRING_TYPE and not isinstance(value, str):
            return False
        elif col['type'] == FLOAT_TYPE and not isinstance(value, float):
            return False
        elif col['type'] == BOOL_TYPE and not isinstance(value, bool):
            return False

        return True


class CatalogManager:
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        self.schemas: Dict[str, Schema] = {}
        self.load_catalog()

    def load_catalog(self):
        """加载系统目录"""
        catalog_file = os.path.join(self.data_dir, 'catalog.json')
        if os.path.exists(catalog_file):
            try:
                with open(catalog_file, 'r') as f:
                    catalog_data = json.load(f)
                    for table_name, schema_data in catalog_data.items():
                        self.schemas[table_name] = Schema(table_name, **schema_data)
            except:
                self.schemas = {}

    def save_catalog(self):
        """保存系统目录"""
        os.makedirs(self.data_dir, exist_ok=True)
        catalog_file = os.path.join(self.data_dir, 'catalog.json')
        catalog_data = {}
        for table_name, schema in self.schemas.items():
            catalog_data[table_name] = {
                'columns': schema.columns,
                'primary_key': schema.primary_key
            }

        with open(catalog_file, 'w') as f:
            json.dump(catalog_data, f, indent=2)

    def create_table(self, table_name: str, columns: List[Dict], primary_key: str = None):
        """创建新表"""
        if table_name in self.schemas:
            raise ValueError(f"Table {table_name} already exists")

        schema = Schema(table_name, columns, primary_key)
        self.schemas[table_name] = schema
        self.save_catalog()
        return schema

    def get_schema(self, table_name: str) -> Optional[Schema]:
        """获取表模式"""
        return self.schemas.get(table_name)

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.schemas