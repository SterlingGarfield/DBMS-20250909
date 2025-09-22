import os
import struct
from typing import Optional
from utils.constants import PAGE_SIZE


class FileManager:
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def get_file_path(self, table_name: str) -> str:
        return os.path.join(self.data_dir, f"{table_name}.dat")

    def delete_file(self, table_name: str) -> bool:
        """删除表文件"""
        file_path = self.get_file_path(table_name)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                return True
            else:
                return False
        except Exception as e:
            print(f"删除文件失败: {e}")
            return False

    def file_exists(self, table_name: str) -> bool:
        return os.path.exists(self.get_file_path(table_name))

    def create_file(self, table_name: str) -> bool:
        file_path = self.get_file_path(table_name)
        if not os.path.exists(file_path):
            with open(file_path, 'wb') as f:
                # 写入文件头（页数）
                f.write(struct.pack('>i', 0))
            return True
        return False

    def read_page(self, table_name: str, page_id: int) -> Optional[bytes]:
        file_path = self.get_file_path(table_name)
        if not os.path.exists(file_path):
            return None

        with open(file_path, 'rb') as f:
            # 跳过文件头
            f.seek(4)
            # 定位到指定页
            f.seek(page_id * PAGE_SIZE, 1)
            return f.read(PAGE_SIZE)

    def write_page(self, table_name: str, page_id: int, data: bytes) -> bool:
        if len(data) != PAGE_SIZE:
            raise ValueError("Page data must be exactly PAGE_SIZE bytes")

        file_path = self.get_file_path(table_name)
        if not os.path.exists(file_path):
            return False

        with open(file_path, 'r+b') as f:
            # 定位到指定页
            f.seek(4 + page_id * PAGE_SIZE)
            f.write(data)
        return True

    def allocate_page(self, table_name: str) -> int:
        file_path = self.get_file_path(table_name)
        if not os.path.exists(file_path):
            return -1

        with open(file_path, 'r+b') as f:
            # 读取当前页数
            num_pages = struct.unpack('>i', f.read(4))[0]
            # 增加页数
            f.seek(0)
            f.write(struct.pack('>i', num_pages + 1))
            # 扩展文件
            f.seek(0, 2)
            f.write(b'\x00' * PAGE_SIZE)

        return num_pages

    def get_page_count(self, table_name: str) -> int:
        file_path = self.get_file_path(table_name)
        if not os.path.exists(file_path):
            return 0

        with open(file_path, 'rb') as f:
            return struct.unpack('>i', f.read(4))[0]