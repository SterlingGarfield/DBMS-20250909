from sql_compiler.catalog import CatalogManager


class DBCatalogManager(CatalogManager):
    def __init__(self, data_dir: str = 'data'):
        super().__init__(data_dir)

    def initialize_system_tables(self):
        """初始化系统表"""
        # 可以在这里创建系统表
        pass