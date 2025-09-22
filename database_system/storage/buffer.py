from typing import Dict, Optional, Tuple, List
from .page import Page
from .file_manager import FileManager
from utils.constants import PAGE_SIZE


class BufferPool:
    def __init__(self, capacity: int, file_manager: FileManager):
        self.capacity = capacity
        self.file_manager = file_manager
        self.pages: Dict[Tuple[str, int], Page] = {}  # (table_name, page_id) -> Page
        self.pin_counts: Dict[Tuple[str, int], int] = {}
        self.dirty_pages: set = set()
        self.lru_list: List[Tuple[str, int]] = []

    def pin_page(self, table_name: str, page_id: int) -> Optional[Page]:
        """固定页到缓冲池"""
        key = (table_name, page_id)

        # 如果页已在缓冲池中
        if key in self.pages:
            page = self.pages[key]
            self.pin_counts[key] += 1
            # 更新LRU
            if key in self.lru_list:
                self.lru_list.remove(key)
            self.lru_list.append(key)
            return page

        # 如果缓冲池已满，需要置换
        if len(self.pages) >= self.capacity:
            self._evict_page()

        # 从磁盘加载页
        page_data = self.file_manager.read_page(table_name, page_id)
        if page_data is None:
            return None

        page = Page.from_bytes(page_id, page_data)
        self.pages[key] = page
        self.pin_counts[key] = 1
        self.lru_list.append(key)

        return page

    def unpin_page(self, table_name: str, page_id: int, is_dirty: bool = False):
        """解除页的固定"""
        key = (table_name, page_id)
        if key not in self.pages:
            return

        self.pin_counts[key] -= 1

        # 标记为脏页
        if is_dirty:
            self.dirty_pages.add(key)
            self.pages[key].dirty = True

        # 如果pin count为0，可以准备置换
        if self.pin_counts[key] == 0:
            # 如果是脏页，立即刷新到磁盘
            if key in self.dirty_pages:
                self.flush_page(table_name, page_id)

            # 更新LRU位置 - 保持原有逻辑
            if key in self.lru_list:
                self.lru_list.remove(key)
            self.lru_list.append(key)  # 最近使用的放在后面

    def flush_page(self, table_name: str, page_id: int):
        """将脏页写回磁盘"""
        key = (table_name, page_id)
        if key not in self.pages or key not in self.dirty_pages:
            return

        page = self.pages[key]

        # 使用正确的方法获取字节数据
        try:
            page_data = page.to_bytes()  # 尝试第一种方法名
        except AttributeError:
            try:
                page_data = page.to_bytes()  # 尝试第二种方法名
            except AttributeError:
                # 如果都没有，直接使用page.data
                page_data = bytes(page.data)


        success = self.file_manager.write_page(table_name, page_id, page_data)
        if success:
            self.dirty_pages.remove(key)
            page.dirty = False


    def flush_all(self):
        """将所有脏页写回磁盘"""
        for key in list(self.dirty_pages):
            table_name, page_id = key
            self.flush_page(table_name, page_id)

    def _evict_page(self):
        """置换最近最少使用的页"""
        # 从LRU列表前面开始找（最久未使用的）
        for key in self.lru_list:
            if self.pin_counts.get(key, 0) == 0:
                table_name, page_id = key
                # 如果是脏页，先写回磁盘
                if key in self.dirty_pages:
                    self.flush_page(table_name, page_id)
                # 从缓冲池移除
                del self.pages[key]
                del self.pin_counts[key]
                self.lru_list.remove(key)
                self.dirty_pages.discard(key)
                return

        raise Exception("Buffer pool full and no unpinned page to evict")

        raise Exception("Buffer pool full and no unpinned page to evict")

    def allocate_page(self, table_name: str) -> Optional[Page]:
        """分配新页"""
        page_id = self.file_manager.allocate_page(table_name)
        if page_id == -1:
            return None

        page = Page(page_id)
        key = (table_name, page_id)
        self.pages[key] = page
        self.pin_counts[key] = 1
        self.lru_list.append(key)
        self.dirty_pages.add(key)

        return page