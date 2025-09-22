from typing import List, Any, Iterator
from sql_compiler.planner import QueryPlan
from .storage_engine import StorageEngine
from sql_compiler.catalog import Schema
from sql_compiler.catalog import CatalogManager


class Executor:
    def __init__(self, storage_engine: StorageEngine, catalog_manager: CatalogManager):
        self.storage_engine = storage_engine
        self.catalog_manager = catalog_manager

    # ... existing code ...

    def execute(self, plan: QueryPlan) -> Any:
        if plan.plan_type == 'SELECT':
            return self._execute_select(plan)
        elif plan.plan_type == 'INSERT':
            return self._execute_insert(plan)
        elif plan.plan_type == 'CREATE_TABLE':
            return self._execute_create_table(plan)
        elif plan.plan_type == 'DROP_TABLE':
            return  self._execute_drop_table(plan)
        else:
            raise ValueError(f"Unsupported plan type: {plan.plan_type}")

    # 添加缺失的 _execute_insert 方法
    def _execute_insert(self, plan: QueryPlan) -> int:
        """执行INSERT语句"""
        table_name = plan.details['table_name']
        values = plan.details['values']
        schema = plan.details['schema']

        # 验证插入的值与表结构匹配
        if len(values) != len(schema.columns):
            raise ValueError(f"列数不匹配: 表有 {len(schema.columns)} 列，但提供了 {len(values)} 个值")

        # 验证数据类型
        for i, (value, col) in enumerate(zip(values, schema.columns)):
            if not schema.validate_value(col['name'], value):
                raise ValueError(f"第 {i + 1} 列 '{col['name']}' 类型不匹配")

        # 插入记录
        record_id = self.storage_engine.insert_record(table_name, schema, values)
        if record_id is None:
            raise Exception("插入记录失败")

        return 1  # 返回插入的行数

    def _execute_drop_table(self, plan: QueryPlan) -> bool:
        """执行DROP TABLE语句"""
        table_name = plan.details['table_name']

        # 1. 从存储引擎删除表文件
        try:
            self.storage_engine.drop_table(table_name)
        except Exception as e:
            print(f"⚠️ 删除表文件失败: {e}")

        # 2. 从目录管理器中删除表元数据
        try:
            if table_name in self.catalog_manager.schemas:
                del self.catalog_manager.schemas[table_name]
                self.catalog_manager.save_catalog()
                return True
            else:
                print(f"⚠️ 表 '{table_name}' 不存在于目录中")
                return False
        except Exception as e:
            print(f"❌ 删除表元数据失败: {e}")
            return False

    def _execute_select(self, plan: QueryPlan) -> List[List[Any]]:
        # ... 现有代码保持不变 ...
        table_name = plan.details['table_name']
        columns = plan.details['columns']
        where_clause = plan.details['where_clause']
        schema = plan.details['schema']

        results = []
        for record in self.storage_engine.scan_records(table_name, schema):
            # 应用WHERE过滤
            if where_clause and not self._evaluate_condition(where_clause, record, schema):
                continue

            # 选择指定列
            if columns == ['*']:
                results.append(record)
            else:
                selected_record = []
                for col_name in columns:
                    col_index = schema.get_column_index(col_name)
                    if col_index != -1:
                        selected_record.append(record[col_index])
                results.append(selected_record)

        return results

    def _execute_create_table(self, plan: QueryPlan) -> bool:
        # ... 现有代码保持不变 ...
        table_name = plan.details['table_name']
        columns = plan.details['columns']
        primary_key = plan.details['primary_key']

        # 创建Schema对象
        schema = Schema(table_name, columns, primary_key)

        # 在catalog中创建表（保存元数据）
        try:
            self.catalog_manager.create_table(table_name, columns, primary_key)
        except ValueError as e:
            # 表已存在
            return False

        # 创建表文件
        success = self.storage_engine.create_table(table_name, schema)
        return success

    def _evaluate_condition(self, condition, record: List[Any], schema: Schema) -> bool:
        """评估WHERE条件"""
        # ... 现有代码保持不变 ...