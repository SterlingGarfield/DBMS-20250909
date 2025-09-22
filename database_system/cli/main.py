#!/usr/bin/env python3
"""
数据库系统命令行接口主程序
"""

import os
import sys
import cmd
import shlex
from typing import List, Optional
from pathlib import Path

# 调试路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(project_root)

from engine.executer import Executor

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from sql_compiler.lexer import Lexer
from sql_compiler.parser import Parser
from sql_compiler.semantic import SemanticAnalyzer
from sql_compiler.planner import Planner
from sql_compiler.catalog import CatalogManager

from storage.file_manager import FileManager
from storage.buffer import BufferPool
from storage.page import Page

from engine.catalog_manager import DBCatalogManager
from engine.storage_engine import StorageEngine


from utils.constants import PAGE_SIZE


class DatabaseCLI(cmd.Cmd):
    """数据库命令行接口"""

    intro = """
    🔷 欢迎使用 LearnDB 数据库系统 🔷
    版本: 1.0.0
    输入 'help' 获取帮助信息，'quit' 退出系统
    """

    prompt = "LearnDB> "

    def __init__(self):
        super().__init__()
        self.data_dir = "data"
        self._initialize_database()

    def _initialize_database(self):
        """初始化数据库系统"""
        try:
            # 创建数据目录
            os.makedirs(self.data_dir, exist_ok=True)

            # 初始化各组件
            self.file_manager = FileManager(self.data_dir)
            self.buffer_pool = BufferPool(capacity=100, file_manager=self.file_manager)
            self.catalog_manager = DBCatalogManager(self.data_dir)
            self.storage_engine = StorageEngine(self.buffer_pool, self.file_manager)

            # 修改：传递catalog_manager给Executor
            self.executor = Executor(self.storage_engine, self.catalog_manager)

            # 初始化编译器组件
            self.lexer = Lexer()
            self.parser = Parser(self.catalog_manager)
            self.semantic_analyzer = SemanticAnalyzer(self.catalog_manager)
            self.planner = Planner(self.catalog_manager)

            print("✅ 数据库系统初始化完成")
            print(f"📁 数据目录: {os.path.abspath(self.data_dir)}")
            print(f"💾 缓冲池大小: 100 页 ({100 * PAGE_SIZE / 1024} KB)")

        except Exception as e:
            print(f"❌ 数据库初始化失败: {e}")
            sys.exit(1)

    def do_quit(self, arg):
        """退出数据库系统: quit"""
        print("👋 感谢使用 LearnDB，再见！")
        self._cleanup()
        return True

    def do_exit(self, arg):
        """退出数据库系统: exit"""
        return self.do_quit(arg)

    def do_EOF(self, arg):
        """Ctrl+D 退出"""
        print()
        return self.do_quit(arg)

    def _cleanup(self):
        """清理资源"""
        try:
            self.buffer_pool.flush_all()
            print("💾 数据已持久化到磁盘")
        except Exception as e:
            print(f"⚠️  清理资源时发生错误: {e}")

    def default(self, line):
        """处理SQL命令"""
        try:
            self._execute_sql(line.strip())
        except Exception as e:
            print(f"❌ 错误: {e}")

    def _execute_sql(self, sql: str):
        """执行SQL语句"""
        if not sql or sql.isspace():
            return

        # 移除末尾分号（如果有）
        if sql.endswith(';'):
            sql = sql[:-1].strip()

        try:
            # 1. 词法分析
            tokens = self.lexer.tokenize(sql)
            if not tokens or (len(tokens) == 1 and tokens[0].type == 'EOF'):
                print("⚠️  空的SQL语句")
                return

            # 2. 语法分析
            ast = self.parser.parse(sql)

            # 3. 语义分析
            validated_ast = self.semantic_analyzer.analyze(ast)

            # 4. 生成执行计划
            plan = self.planner.create_plan(validated_ast)

            # 5. 执行计划
            result = self.executor.execute(plan)

            # 6. 显示结果
            self._display_result(result, plan)

        except Exception as e:
            print(f"❌ SQL执行错误: {e}")
            # 打印详细的错误信息（用于调试）
            import traceback
            traceback.print_exc()

    def _display_result(self, result, plan):
        """显示查询结果"""
        if plan.plan_type == 'SELECT':
            if not result:
                print("📊 查询结果: 0 行")
                return

            print(f"📊 查询结果: {len(result)} 行")
            for i, row in enumerate(result, 1):
                print(f"{i:3d} | {' | '.join(str(x) for x in row)}")

        elif plan.plan_type == 'INSERT':
            print(f"✅ 插入成功: 影响了 {result} 行")

        elif plan.plan_type == 'CREATE_TABLE':
            print(f"✅ 表创建成功: {plan.details['table_name']}")

        else:
            print(f"✅ 操作完成: {result}")

    def do_tables(self, arg):
        """显示所有表: tables"""
        try:
            tables = list(self.catalog_manager.schemas.keys())
            if not tables:
                print("📋 数据库中没有表")
                return

            print("📋 数据库中的表:")
            for i, table in enumerate(tables, 1):
                schema = self.catalog_manager.get_schema(table)
                print(f"{i:2d}. {table} ({len(schema.columns)} 列)")

        except Exception as e:
            print(f"❌ 获取表列表失败: {e}")

    def do_desc(self, arg):
        """显示表结构: desc <table_name>"""
        if not arg:
            print("❌ 请指定表名: desc <table_name>")
            return

        table_name = arg.strip()
        try:
            schema = self.catalog_manager.get_schema(table_name)
            if not schema:
                print(f"❌ 表 '{table_name}' 不存在")
                return

            print(f"📋 表结构: {table_name}")
            print("┌──────┬────────────┬────────┬────────┐")
            print("│ 序号 │ 列名       │ 类型   │ 长度   │")
            print("├──────┼────────────┼────────┼────────┤")

            for i, col in enumerate(schema.columns, 1):
                col_name = col['name']
                col_type = col['type']
                col_length = col.get('length', '')
                if col_type == 'INT':
                    col_length = ''
                print(f"│ {i:4d} │ {col_name:10} │ {col_type:6} │ {str(col_length):6} │")

            print("└──────┴────────────┴────────┴────────┘")

            if schema.primary_key:
                print(f"🔑 主键: {schema.primary_key}")

        except Exception as e:
            print(f"❌ 获取表结构失败: {e}")

    def do_clear(self, arg):
        """清空屏幕: clear"""
        os.system('cls' if os.name == 'nt' else 'clear')

    def do_stats(self, arg):
        """显示数据库统计信息: stats"""
        try:
            # 缓冲池统计
            buffer_stats = {
                'total_pages': len(self.buffer_pool.pages),
                'dirty_pages': len(self.buffer_pool.dirty_pages),
                'pinned_pages': sum(1 for count in self.buffer_pool.pin_counts.values() if count > 0)
            }

            # 表统计
            table_stats = []
            for table_name in self.catalog_manager.schemas.keys():
                page_count = self.file_manager.get_page_count(table_name)
                table_stats.append((table_name, page_count))

            print("📈 数据库统计信息:")
            print(f"💾 缓冲池: {buffer_stats['total_pages']} 页, "
                  f"{buffer_stats['dirty_pages']} 脏页, "
                  f"{buffer_stats['pinned_pages']} 固定页")

            print("📊 表信息:")
            for table_name, page_count in table_stats:
                print(f"  {table_name}: {page_count} 页")

        except Exception as e:
            print(f"❌ 获取统计信息失败: {e}")

    def do_shell(self, arg):
        """执行系统命令: shell <command>"""
        if not arg:
            print("❌ 请指定要执行的命令")
            return

        try:
            os.system(arg)
        except Exception as e:
            print(f"❌ 执行命令失败: {e}")

    def do_help(self, arg):
        """显示帮助信息: help [command]"""
        if arg:
            # 显示特定命令的帮助
            super().do_help(arg)
        else:
            # 显示所有命令帮助
            print("\n📖 LearnDB 命令帮助:")
            print("=" * 50)
            print("SQL 命令:")
            print("  SELECT * FROM table_name [WHERE condition];")
            print("  INSERT INTO table_name VALUES (value1, value2, ...);")
            print("  CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...);")
            print()
            print("系统命令:")
            print("  tables              - 显示所有表")
            print("  desc <table_name>   - 显示表结构")
            print("  stats               - 显示统计信息")
            print("  clear               - 清空屏幕")
            print("  shell <command>     - 执行系统命令")
            print("  help [command]      - 显示帮助信息")
            print("  quit/exit           - 退出系统")
            print("=" * 50)


def main():
    """主函数"""
    print("🚀 正在启动 LearnDB 数据库系统...")

    try:
        cli = DatabaseCLI()
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\n👋 用户中断，正在退出...")
    except Exception as e:
        print(f"💥 系统发生严重错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("✅ 系统已安全关闭")


if __name__ == "__main__":
    main()