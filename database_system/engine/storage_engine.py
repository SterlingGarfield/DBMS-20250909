from typing import List, Optional, Any, Iterator
from storage.buffer import BufferPool
from storage.file_manager import FileManager
from sql_compiler.catalog import Schema
from utils.helpers import *


class StorageEngine:
    def __init__(self, buffer_pool: BufferPool, file_manager: FileManager):
        self.buffer_pool = buffer_pool
        self.file_manager = file_manager

    def create_table(self, table_name: str, schema: Schema) -> bool:
        """创建新表文件"""
        return self.file_manager.create_file(table_name)

    def drop_table(self, table_name: str) -> bool:
        """删除表文件"""
        try:
            # 从缓冲池中移除所有相关页面
            self._remove_table_pages_from_buffer(table_name)

            # 删除表文件
            return self.file_manager.delete_file(table_name)
        except Exception as e:
            print(f"❌ 删除表文件失败: {e}")
            return False

    def _remove_table_pages_from_buffer(self, table_name: str):
                """从缓冲池中移除指定表的所有页面"""
                # 收集要移除的键
                keys_to_remove = []
                for key in list(self.buffer_pool.pages.keys()):
                    if key[0] == table_name:
                        keys_to_remove.append(key)

                # 移除页面
                for key in keys_to_remove:
                    table_name, page_id = key
                    # 如果是脏页，先刷新到磁盘
                    if key in self.buffer_pool.dirty_pages:
                        self.buffer_pool.flush_page(table_name, page_id)
                    # 从缓冲池中移除
                    if key in self.buffer_pool.pages:
                        del self.buffer_pool.pages[key]
                    if key in self.buffer_pool.pin_counts:
                        del self.buffer_pool.pin_counts[key]

                # 从LRU列表中移除
                self.buffer_pool.lru_list = [key for key in self.buffer_pool.lru_list if key[0] != table_name]

                # 从脏页集合中移除
                self.buffer_pool.dirty_pages = {key for key in self.buffer_pool.dirty_pages if key[0] != table_name}

    def insert_record(self, table_name: str, schema: Schema, values: List[Any]) -> Optional[int]:
        """插入记录"""
        # 序列化记录
        record_data = self._serialize_record(schema, values)

        # 尝试在现有页中插入
        page_count = self.file_manager.get_page_count(table_name)
        for page_id in range(page_count):
            page = self.buffer_pool.pin_page(table_name, page_id)
            if page and page.has_free_space(len(record_data)):
                record_id = page.insert_record(record_data)
                self.buffer_pool.unpin_page(table_name, page_id, True)
                if record_id is not None:
                    return (page_id << 16) | record_id  # 组合页ID和记录ID

        # 需要分配新页
        new_page = self.buffer_pool.allocate_page(table_name)
        if new_page:
            record_id = new_page.insert_record(record_data)
            self.buffer_pool.unpin_page(table_name, new_page.page_id, True)
            if record_id is not None:
                return (new_page.page_id << 16) | record_id

        return None

    def scan_records(self, table_name: str, schema: Schema) -> Iterator[List[Any]]:
        """扫描所有记录"""
        page_count = self.file_manager.get_page_count(table_name)
        record_size = self._calculate_record_size(schema)

        for page_id in range(page_count):
            page = self.buffer_pool.pin_page(table_name, page_id)
            if page:
                for record_id in range(page.num_records):
                    record_data = page.get_record(record_id, record_size)
                    if record_data:
                        record = self._deserialize_record(schema, record_data)
                        yield record
                self.buffer_pool.unpin_page(table_name, page_id, False)

    def _serialize_record(self, schema: Schema, values: List[Any]) -> bytes:
        """序列化记录"""
        record_data = bytearray()

        for value, col_def in zip(values, schema.columns):
            if value is None:
                # 处理NULL值 - 填充适当大小的空字节
                size = get_type_size(col_def['type'], col_def.get('length', 0))
                record_data.extend(b'\x00' * size)
            elif col_def['type'] == 'INT':
                record_data.extend(serialize_int(value))
            elif col_def['type'] == 'VARCHAR':
                record_data.extend(serialize_string(value, col_def.get('length', 255)))
            else:
                # 未知类型，填充空字节
                size = get_type_size(col_def['type'], col_def.get('length', 0))
                record_data.extend(b'\x00' * size)

        return bytes(record_data)

    def _deserialize_record(self, schema: Schema, record_data: bytes) -> List[Any]:
        """反序列化记录"""
        record = []
        offset = 0

        for col_def in schema.columns:
            col_type = col_def['type']
            col_length = col_def.get('length', 0)
            size = get_type_size(col_type, col_length)

            if offset + size > len(record_data):
                # 数据不完整，填充NULL
                record.append(None)
                continue

            chunk = record_data[offset:offset + size]

            # 检查是否为NULL值（全0字节）
            if all(b == 0 for b in chunk):
                value = None
            elif col_type == 'INT':
                try:
                    value = deserialize_int(chunk)
                except:
                    value = None
            elif col_type == 'VARCHAR':
                try:
                    value = deserialize_string(chunk)
                except:
                    value = None
            else:
                value = None

            record.append(value)
            offset += size

        return record

    def _calculate_record_size(self, schema: Schema) -> int:
        """计算记录大小"""
        size = 0
        for col_def in schema.columns:
            size += get_type_size(col_def['type'], col_def.get('length', 0))
        return size