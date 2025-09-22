import struct
from typing import List, Optional, Tuple
from utils.constants import PAGE_SIZE, RECORD_SIZE
from utils.helpers import serialize_int, deserialize_int, serialize_string, deserialize_string


class Page:
    def __init__(self, page_id: int):
        self.page_id = page_id
        self.data = bytearray(PAGE_SIZE)
        self.num_records = 0
        self.free_space_start = 8  # 页头占8字节
        self.record_offsets = []  # 存储每个记录的偏移量
        self.dirty = False

        # 初始化页头
        self._init_header()

    def _init_header(self):
        """初始化页头信息"""
        # 页头格式: [num_records(4B), free_space_start(4B)]
        struct.pack_into('>ii', self.data, 0, 0, 8)
        self.free_space_start = 8
        self.record_offsets = []

    def read_header(self):
        """读取页头信息"""
        # 只读取前8字节的页头
        header_data = self.data[0:8]
        self.num_records, self.free_space_start = struct.unpack('>ii', header_data)

    def _reconstruct_offsets(self):
        """重建记录偏移量（简化版本）"""
        # 这是一个临时解决方案，更好的方法是在页头存储偏移量信息
        offsets = []
        current_offset = 8  # 跳过页头

        # 由于我们不知道记录大小，这个函数需要更多上下文信息
        # 暂时返回空列表，需要在外部处理
        return []

    def write_header(self):
        """写入页头信息"""
        # 只写入前8字节的页头，不要覆盖后面的数据
        header_data = struct.pack('>ii', self.num_records, self.free_space_start)
        self.data[0:8] = header_data  # 只修改前8字节
        self.dirty = True

    def has_free_space(self, record_size: int) -> bool:
        """检查是否有足够空间存放记录"""
        return (PAGE_SIZE - self.free_space_start) >= record_size

    def insert_record(self, record_data: bytes) -> Optional[int]:
        """插入记录，返回记录ID"""
        record_size = len(record_data)
        if not self.has_free_space(record_size):
            return None

        # 使用固定偏移量策略
        record_offset = 8 + self.num_records * record_size


        # 写入记录
        self.data[record_offset:record_offset + record_size] = record_data

        # 更新页头
        self.num_records += 1
        self.free_space_start = 8 + self.num_records * record_size
        self.write_header()
        self.dirty = True

        return self.num_records - 1

    def get_record(self, record_id: int, record_size: int) -> Optional[bytes]:
        """获取指定记录"""
        if record_id >= self.num_records:
            return None

        # 临时解决方案：使用固定偏移量计算
        # 页头占8字节，然后按记录大小顺序存储
        record_offset = 8 + record_id * record_size

        if record_offset + record_size > PAGE_SIZE:
            return None

        record_data = bytes(self.data[record_offset:record_offset + record_size])

        return record_data

    @classmethod
    def from_bytes(cls, page_id: int, data: bytes):
        """从字节数据创建页"""
        if len(data) != PAGE_SIZE:
            raise ValueError("Invalid page data size")

        page = cls(page_id)
        page.data = bytearray(data)
        page.read_header()
        return page