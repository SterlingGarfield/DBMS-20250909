import java.io.*;
import java.util.*;

/**
 * 类描述 接口实现类，实现了具体接口，供其他类调用
 *
 */
class API {
	//// ****Fields****////
	private String current_database;

	//// ****Methods****////
	// **constructors**//
	public API() {
		current_database = "";
	}

	// **Accessors**//
	public String getCurrentDatabase() {
		return current_database;
	}

	// **Mutators**//
	public void setCurrentDatabase(String db) {
		current_database = db;
	}

	// **Functions**//
	// create a new database use Catalog.createDatabase() to create .cat file
	// and Index_Manage.createDatabase() to create .idx file
	public int createDatabase(String database_name) throws Exception {
		if (!checkDatabase(database_name)) {

			File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator + database_name);
			ff.mkdir();
			HNUSQL.cm.createDatabase(database_name); // 初始化数据库信息，存在cat文件中
			HNUSQL.im.createDatabase(database_name);
			System.out.println("create database <" + database_name + ">");

			return 0;
		} else {
			System.out.println("*****database <" + database_name + "> already exist!*****");
			return -1;
		}
	}

	/**
	 * 1. create a new table use Catalog_Manager.createTable to store table
	 * information 2. and Record_Manager.createTable to create .dat file 3. and
	 * Index_Manager.createIndex to create index for primary key
	 * 
	 * @param table_name
	 * @param attr_name
	 * @param attr_type
	 * @param is_unique
	 * @param primary_key
	 * @return
	 * @throws Exception
	 */
	public int createTable(String table_name, String[] attr_name, int[] attr_type, boolean[] is_unique,
			String primary_key) throws Exception {
		if (this.getCurrentDatabase() == "") {
			System.out.println("*****Please Select A Database First*****");
			return -1;
		} else if (HNUSQL.cm.checkTable(table_name)) {
			System.out.println("*****Table already Exist!*****");
			return -1;
		} else {
			int j;
			for (j = 0; j < attr_name.length; j++)
				if (attr_name[j].equals(primary_key))
					break;
			is_unique[j] = true;
			System.out.print("create table <" + table_name + ">\t");
			for (int i = 0; i < attr_name.length; i++)
				if (attr_type[i] == -1)
					System.out.print("attribute " + i + " <" + attr_name[i] + "><int> unique<" + is_unique[i] + ">\t");
				else if (attr_type[i] == -2)
					System.out
							.print("attribute " + i + " <" + attr_name[i] + "><float> unique<" + is_unique[i] + ">\t");
				else if (attr_type[i] > 0)
					System.out.print("attribute " + i + " <" + attr_name[i] + "><char><" + attr_type[i] + "> unique<"
							+ is_unique[i] + ">\t");
			System.out.print("primary key <" + primary_key + ">\n");

			HNUSQL.cm.createTable(table_name, attr_name, attr_type, is_unique, primary_key);
			HNUSQL.rm.createTable(table_name);
			// TODO：索引未实现
			HNUSQL.im.createIndex("idx" + primary_key, table_name, primary_key);

			return 0;
		}
	}

	public int createIndex(String index_name, String table_name, String attr_name) {
		if (getCurrentDatabase() == "") {
			System.out.println("*****Please select a database first!*****");
			return -1;
		} else if (HNUSQL.cm.checkAttribute(table_name, attr_name)) {
			System.out.println("*****Attribute not exist!*****");
			return -1;
		} else if (HNUSQL.cm.checkIndex_name(index_name)) {
			System.out.println("*****Index name exist!*****");
			return -1;
		} else if (HNUSQL.cm.checkIndex(table_name, attr_name)) {
			System.out.println("*****Index already exist on <" + table_name + "> <" + attr_name + ">!*****");
			return -1;
		} else if (HNUSQL.cm.checkUnique(table_name, attr_name)) {
			System.out.println("*****Attribute is not Unique!*****");
			return -1;
		} else {

			System.out.println(
					"create index <" + index_name + "> on table <" + table_name + "> attribute <" + attr_name + ">");
			HNUSQL.im.createIndex(index_name, table_name, attr_name);
			HNUSQL.cm.createIndex(index_name, table_name, attr_name);

			return 0;
		}
	}

	public int useDatabase(String database_name) throws Exception {
		if (!checkDatabase(database_name)) {
			System.out.println("*****Database not Found*****");
			return -1;
		} else if (getCurrentDatabase() == database_name) {
			System.out.println("*****Database already in Using*****");
			return -1;
		} else {
			System.out.println("use database <" + database_name + ">");
			HNUSQL.bm.UseDatabase(database_name);
			HNUSQL.cm.UseDatabase(database_name);
			setCurrentDatabase(database_name);

			return 0;
		}
	}

	public int dropDatabase(String database_name) throws Exception {
		if (!checkDatabase(database_name)) {
			System.out.println("*****Database <" + database_name + "> is no exit");
			return -1;
		} else {
			HNUSQL.rm.dropDatabase(database_name);
			HNUSQL.im.dropDatabase(database_name);
			HNUSQL.cm.dropDatabase(database_name);
			File dir = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator + database_name);
			dir.delete();
			System.out.println("drop database <" + database_name + ">");

		}
		if (getCurrentDatabase().equals(database_name))
			setCurrentDatabase("");

		return 0;
	}

	public int dropTable(String table_name) {
		if (getCurrentDatabase() == "") {
			System.out.println("*****Please select a database first!*****");
			return -1;
		} else if (!HNUSQL.cm.checkTable(table_name)) {
			System.out.println("*****Table <" + table_name + "> not exit!*****");
			return -1;
		} else {
			System.out.println("drop table <" + table_name + ">");
			HNUSQL.rm.dropTable(table_name);
			String[] index;
			index = HNUSQL.cm.getIndex_name(table_name);
			for (int i = 0; i < index.length; i++)
				HNUSQL.im.dropIndex(index[i]);

			// HNUSQL.rm.dropTable(table_name);
			HNUSQL.cm.dropTable(table_name);

			return 0;
		}
	}

	public int dropIndex(String index_name) {
		if (!HNUSQL.cm.checkIndex_name(index_name)) {
			System.out.println("*****Index <" + index_name + "> not exist*****");
			return -1;
		} else {
			System.out.println("drop index <" + index_name + ">");
			HNUSQL.im.dropIndex(index_name);
			HNUSQL.cm.dropIndex(index_name);

			return 0;
		}
	}

	// attribute type---- -1 : int; -2 : float; n : char(n) -------

	/**
	 * 插入数据到记录
	 * 
	 * @param table_name 表名
	 * @param attribute  字段值
	 * @param attr_type  字段类型
	 * @return
	 * @throws Exception
	 */
	public int insertRecord(String table_name, String[] attribute, int[] attr_type) throws Exception {
		if (getCurrentDatabase() == "") {
			System.out.println("*****Please select a database first!*****");
			return -1;
		} else if (!HNUSQL.cm.checkTable(table_name)) {
			System.out.println("*****Table <" + table_name + "> not exist*****");
			return -1;
		} else {
			if (!HNUSQL.cm.checkAttributeNum(table_name, attribute.length)) {
				System.out.println("*****Parameter number wrong!*****");
				return -1;
			} else {
				for (int i = 0; i < attribute.length; i++)
					if (!HNUSQL.cm.checkAttributeType(table_name, i, attr_type[i])) {
						System.out.println("*****Parameter " + i + " Type Error*****");
						return -1;
					}
				System.out.print("insert into table <" + table_name + ">\t");
				for (int i = 0; i < attribute.length; i++)
					if (attr_type[i] == -1)
						System.out.print("value " + i + " : int<" + Integer.decode(attribute[i]).intValue() + ">\t");
					else if (attr_type[i] == -2)
						System.out.print("value " + i + " : float<" + Float.parseFloat(attribute[i]) + ">\t");
					else
						System.out.print("value " + i + " : char<" + attr_type[i] + "><" + attribute[i] + ">\t");
				// 将字段值数组 attribute 转化为byte数组
				byte[] data = RecordTobyte(attribute, HNUSQL.cm.getAttributeTypes(table_name));
				// 插入到Record
				HNUSQL.rm.insertRecord(table_name, data);

				return 0;

			}
		}
	}

	/**
	 * 非条件删除 --删除所有数据
	 * 
	 * @param table_name
	 * @return
	 * @throws Exception
	 */
	public int deleteRecord(String table_name) throws Exception {
		if (getCurrentDatabase() == "") {
			System.out.println("*****Please select a database first!*****");
			return -1;
		} else if (!HNUSQL.cm.checkTable(table_name)) {
			System.out.println("*****Table <" + table_name + "> not exist*****");
			return -1;
		} else {
			System.out.println("delete all records in table <" + table_name + ">");
			int count = HNUSQL.rm.getNum_of_Record(table_name);
			int length = HNUSQL.cm.getRecord_length(table_name);
			byte[] data = new byte[length];
			System.out.println("Delete:");
			while (count > 0) {
				int ii = 0;
				data = HNUSQL.rm.getRecord(table_name, ii, length);
				int[] kk = HNUSQL.cm.getAttributeTypes(table_name);
				String[] ss = ByteToRecord(data, kk);
				for (int k = 0; k < ss.length; k++)
					System.out.print(ss[k] + "\t");
				System.out.print("\tdeleted\n");
				HNUSQL.rm.deleteRecord(table_name, ii, length);
				count = HNUSQL.rm.getNum_of_Record(table_name);
			}
			return 0;
		}
	}

	/**
	 * 条件删除，删除满足条件的数据
	 * 
	 * @param table_name
	 * @param con_attr_name
	 * @param con_attribute
	 * @param con_attr_type
	 * @param operation
	 * @return
	 * @throws Exception
	 */
	public int deleteRecord(String table_name, String[] con_attr_name, String[] con_attribute, int[] con_attr_type,
			int[] operation) throws Exception {
		if (getCurrentDatabase() == "") {
			System.out.println("*****Please select a database first!*****");
			return -1;
		} else if (!HNUSQL.cm.checkTable(table_name)) {
			System.out.println("*****Table <" + table_name + "> not exist*****");
			return -1;
		} else {
			for (int i = 0; i < con_attr_name.length; i++) {
				if (!HNUSQL.cm.checkAttribute(table_name, con_attr_name[i])) {
					System.out.println("*****Attribute <" + (con_attr_name[i] + 1) + "> not exist*****");
					return -1;
				} else {
					int type = HNUSQL.cm.getAttributeType(table_name, con_attr_name[i]);
					if (type == -1 | type == -2) {
						if (con_attr_type[i] != type) {
							System.out.println("*****Attribute type error*****");
							return -1;
						}
					} else if (type >= 0) {
						if (con_attr_type[i] > type) {
							System.out.println("*****Attribute type error*****");
							return -1;
						}
					}
				}
			}

			String s;
			System.out.print("\ndelete from table <" + table_name + "> under conditions:\n");
			for (int i = 0; i < con_attr_name.length; i++) {
				switch (operation[i]) {
				case 1:
					s = "=";
					break;
				case 2:
					s = "<>";
					break;
				case 3:
					s = "<";
					break;
				case 4:
					s = ">";
					break;
				case 5:
					s = "<=";
					break;
				case 6:
					s = ">=";
					break;
				default:
					s = "?";
				}
				if (con_attr_type[i] == -1)
					System.out.print("condition attribute " + i + " <" + con_attr_name[i] + "> " + s + " int<"
							+ Integer.decode(con_attribute[i]).intValue() + ">\t");
				else if (con_attr_type[i] == -2)
					System.out.print("condition attribute " + i + " <" + con_attr_name[i] + "> " + s + " float<"
							+ Float.parseFloat(con_attribute[i]) + ">\t");
				else
					System.out.print("condition attribute " + i + " <" + con_attr_name[i] + "> " + s + " char<"
							+ con_attr_type[i] + "><" + con_attribute[i] + ">\t");
			}

			int count = HNUSQL.rm.getNum_of_Record(table_name);
			int length = HNUSQL.cm.getRecord_length(table_name);
			byte[] data = new byte[length];
			System.out.println("\nDelete:");
			/**
			 * 查询所有记录，根据条件，删除符合条件的记录
			 */
			for (int i = 0; i < count; i++) {
				data = HNUSQL.rm.getRecord(table_name, i, length);
				int j;
				/**
				 * 检查所有的条件是否满足
				 */
				for (j = 0; j < con_attr_name.length; j++)
					if (!checkCondition(table_name, data, con_attr_name[j], con_attribute[j], operation[j]))
						break;

				if (j == con_attr_name.length) {
					int[] kk = HNUSQL.cm.getAttributeTypes(table_name);
					String[] ss = ByteToRecord(data, kk);
					for (int k = 0; k < ss.length; k++)
						System.out.print(ss[k] + "\t");
					System.out.print("\tdeleted\n");

					HNUSQL.rm.deleteRecord(table_name, i, length);
					count = HNUSQL.rm.getNum_of_Record(table_name);
				}

			}
			return 0;
		}
	}

	/**
	 * 查询
	 * 
	 * @param table_name
	 * @return
	 * @throws Exception
	 */
	public int selectRecord(String table_name) throws Exception {
		if (getCurrentDatabase() == "") {
			System.out.println("*****Please select a database first!*****");
			return -1;
		} else if (!HNUSQL.cm.checkTable(table_name)) {
			System.out.println("*****Table <" + table_name + "> not exist*****");
			return -1;
		} else {

			int count = HNUSQL.rm.getNum_of_Record(table_name);
			int length = HNUSQL.cm.getRecord_length(table_name);
			byte[] data = new byte[length];
			for (int i = 0; i < count; i++) {
				data = HNUSQL.rm.getRecord(table_name, i, length); // 获取数据
				int[] kk = HNUSQL.cm.getAttributeTypes(table_name);
				String[] ss = ByteToRecord(data, kk);
				for (int k = 0; k < ss.length; k++)
					System.out.print(ss[k] + "\t");
				System.out.print("\n");

			}

			System.out.println("select all records from table <" + table_name + ">");

			return 0;
		}
	}

	/**
	 * 条件查询
	 * 
	 * @param table_name
	 * @param con_attr_name
	 * @param con_attribute
	 * @param con_attr_type
	 * @param operation
	 * @return
	 * @throws Exception
	 */
	public int selectRecord(String table_name, String[] con_attr_name, String[] con_attribute, int[] con_attr_type,
			int[] operation) throws Exception {
		if (getCurrentDatabase() == "") {
			System.out.println("*****Please select a database first!*****");
			return -1;
		} else if (!HNUSQL.cm.checkTable(table_name)) {
			System.out.println("*****Table <" + table_name + "> not exist*****");
			return -1;
		} else {
			for (int i = 0; i < con_attr_name.length; i++) {
				if (!HNUSQL.cm.checkAttribute(table_name, con_attr_name[i])) {
					System.out.println("*****Attribute <" + (con_attr_name[i] + 1) + "> not exist*****");
					return -1;
				} else {
					int type = HNUSQL.cm.getAttributeType(table_name, con_attr_name[i]);
					if (type == -1 | type == -2) {
						if (con_attr_type[i] != type) {
							System.out.println("*****Attribute type error*****");
							return -1;
						}
					} else if (type >= 0) {
						if (con_attr_type[i] > type) {
							System.out.println("*****Attribute type error*****");
							return -1;
						}
					}
				}
			}

			String s;
			System.out.print("\nselect records from table <" + table_name + "> under conditions:\n");
			for (int i = 0; i < con_attr_name.length; i++) {
				switch (operation[i]) {
				case 1:
					s = "=";
					break;
				case 2:
					s = "<>";
					break;
				case 3:
					s = "<";
					break;
				case 4:
					s = ">";
					break;
				case 5:
					s = "<=";
					break;
				case 6:
					s = ">=";
					break;
				default:
					s = "?";
				}
				if (con_attr_type[i] == -1)
					System.out.print("condition attribute " + i + " <" + con_attr_name[i] + "> " + s + " int<"
							+ Integer.decode(con_attribute[i]).intValue() + ">\t");
				else if (con_attr_type[i] == -2)
					System.out.print("condition attribute " + i + " <" + con_attr_name[i] + "> " + s + " float<"
							+ Float.parseFloat(con_attribute[i]) + ">\t");
				else
					System.out.print("condition attribute " + i + " <" + con_attr_name[i] + "> " + s + " char<"
							+ con_attr_type[i] + "><" + con_attribute[i] + ">\t");
			}

			int count = HNUSQL.rm.getNum_of_Record(table_name);
			int length = HNUSQL.cm.getRecord_length(table_name);
			byte[] data = new byte[length];
			System.out.println("");
			for (int i = 0; i < count; i++) {
				data = HNUSQL.rm.getRecord(table_name, i, length);
				int j = 0;
				for (j = 0; j < con_attr_name.length; j++)
					if (!checkCondition(table_name, data, con_attr_name[j], con_attribute[j], operation[j]))
						break;
				if (j == con_attr_name.length) {
					int[] kk = HNUSQL.cm.getAttributeTypes(table_name);
					String[] ss = ByteToRecord(data, kk);
					for (int k = 0; k < ss.length; k++)
						System.out.print(ss[k] + "\t");
					System.out.print("\n");
				}

			}

			return 0;
		}

	}

	/**
	 * 更新
	 * 
	 * @return
	 * @throws Exception
	 */
	public int updateRecord(String table_name, String target_name, String target_value, int target_type,
			String[] con_attr_name, String[] con_attr, int[] con_attr_type, int[] operation) throws Exception {
		if (getCurrentDatabase() == "") {
			System.out.println("*****Please select a database first!*****");
			return -1;
		} else if (!HNUSQL.cm.checkTable(table_name)) {
			System.out.println("*****Table <" + table_name + "> not exist*****");
			return -1;
		} else {
			// 1. 先查出所有记录
			int count = HNUSQL.rm.getNum_of_Record(table_name);
			int length = HNUSQL.cm.getRecord_length(table_name);
			byte[] data;
			// System.out.println(" ");
			for (int i = 0; i < count; i++) {
				data = HNUSQL.rm.getRecord(table_name, i, length);
				int j = 0;
				for (j = 0; j < 1; j++)
					if (!checkCondition(table_name, data, con_attr_name[j], con_attr[j], operation[j]))
						break;
				if (j == 1) {
					int[] kk = HNUSQL.cm.getAttributeTypes(table_name);
					String[] ss = ByteToRecord(data, kk);
					int target_index = HNUSQL.cm.getAttributeNO(table_name, target_name); // 寻找要修改的目标位置
					ss[target_index] = target_value; // 修改为目标值
					// 删除原记录
					HNUSQL.rm.deleteRecord(table_name, i, length);
					byte[] new_data = RecordTobyte(ss, HNUSQL.cm.getAttributeTypes(table_name));
					// 插入到Record
					HNUSQL.rm.insertRecord(table_name, new_data);
					return 0; // 加该行只修改第一条符合条件的记录
				}

			}

			return 0;
		}

	}

	public boolean checkDatabase(String database_name) {
		File dir = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator + database_name);
		return dir.exists();
	}

	public boolean checkCondition(String table_name, byte[] data, String con_attr_name, String con_attribute,
			int operation) {
		int i;
		int type;
		int[] types;

		types = HNUSQL.cm.getAttributeTypes(table_name);
		String[] ss = ByteToRecord(data, types);

		i = HNUSQL.cm.getAttributeNO(table_name, con_attr_name);
		type = HNUSQL.cm.getAttributeType(table_name, con_attr_name);
		if (operation == 1) {// 等于
			if (type == -1)
				return (Integer.decode(con_attribute).intValue() == Integer.decode(ss[i]).intValue());
			else if (type == -2)
				return (Float.parseFloat(con_attribute) == Float.parseFloat(ss[i]));
			else if (type >= 0)
				return (con_attribute.equals(Interpreter.clean(ss[i])));
		} else if (operation == 2) {// 不等于
			if (type == -1)
				return (Integer.decode(con_attribute).intValue() != Integer.decode(ss[i]).intValue());
			else if (type == -2)
				return (Float.parseFloat(con_attribute) != Float.parseFloat(ss[i]));
			else if (type >= 0)
				return (!con_attribute.equals(ss[i]));
		} else if (operation == 3) { // 小于
			if (type == -1)
				return (Integer.decode(ss[i]).intValue() < Integer.decode(con_attribute).intValue());
			else if (type == -2)
				return (Float.parseFloat(ss[i]) < Float.parseFloat(con_attribute));
			else if (type >= 0)
				return (ss[i].length() < con_attribute.length());
		} else if (operation == 4) {// 大于
			if (type == -1)
				return (Integer.decode(ss[i]).intValue() > Integer.decode(con_attribute).intValue());
			else if (type == -2)
				return (Float.parseFloat(ss[i]) > Float.parseFloat(con_attribute));
			else if (type >= 0)
				return (ss[i].length() > con_attribute.length());
		} else if (operation == 5) { // 小于等于
			if (type == -1)
				return (Integer.decode(ss[i]).intValue() <= Integer.decode(con_attribute).intValue());
			else if (type == -2)
				return (Float.parseFloat(ss[i]) <= Float.parseFloat(con_attribute));
			else if (type >= 0)
				return (ss[i].length() <= con_attribute.length());
		} else if (operation == 6) {// 大于等于
			if (type == -1)
				return (Integer.decode(ss[i]).intValue() >= Integer.decode(con_attribute).intValue());
			else if (type == -2)
				return (Float.parseFloat(ss[i]) >= Float.parseFloat(con_attribute));
			else if (type >= 0)
				return (ss[i].length() >= con_attribute.length());
		}
		return false;
	}

	// encode the record to a byte array according to the attribute types of the
	// record
	public byte[] RecordTobyte(String[] attribute, int[] attr_type) {

		byte[] bb = new byte[0];
		byte[] b;
		for (int i = 0; i < attribute.length; i++) {
			if (attr_type[i] == -1) {
				b = intTobyte(Integer.decode(attribute[i]).intValue());
				byte[] bbb = new byte[bb.length + 4];
				for (int j = 0; j < bb.length; j++)
					bbb[j] = bb[j];
				bbb[bbb.length - 4] = b[0];
				bbb[bbb.length - 3] = b[1];
				bbb[bbb.length - 2] = b[2];
				bbb[bbb.length - 1] = b[3];
				bb = bbb;
			} else if (attr_type[i] == -2) {
				b = floatTobyte(Float.parseFloat(attribute[i]));
				byte[] bbb = new byte[bb.length + 4];
				for (int j = 0; j < bb.length; j++)
					bbb[j] = bb[j];
				bbb[bbb.length - 4] = b[0];
				bbb[bbb.length - 3] = b[1];
				bbb[bbb.length - 2] = b[2];
				bbb[bbb.length - 1] = b[3];
				bb = bbb;
			} else if (attr_type[i] >= 0) {
				char[] c = attribute[i].toCharArray();
				b = new byte[attr_type[i] * 2];
				for (int k = 0; k < c.length; k++) {
					b[k * 2] = charTobyte(c[k])[0];
					b[k * 2 + 1] = charTobyte(c[k])[1];
				}
				if (c.length * 2 < b.length)
					for (int k = 0; k < (b.length - c.length * 2) / 2; k++) {
						b[c.length * 2 + k * 2] = charTobyte(' ')[0];
						b[c.length * 2 + k * 2 + 1] = charTobyte(' ')[1];
					}
				byte[] bbb = new byte[bb.length + b.length];
				for (int j = 0; j < bb.length; j++)
					bbb[j] = bb[j];
				for (int j = 0; j < b.length; j++)
					bbb[bb.length + j] = b[j];
				bb = bbb;
			}
		}
		return bb;
	}

	// decode the byte array to a record according to the array types of record
	public String[] ByteToRecord(byte[] data, int[] type) {
		String[] s = new String[type.length];
		int position = 0;
		byte[] temp;

		for (int i = 0; i < type.length; i++) {
			if (type[i] == -1) {
				temp = new byte[4];
				for (int j = 0; j < 4; j++)
					temp[j] = data[position + j];
				s[i] = new Integer(byteToint(temp)).toString();
				position += 4;
			} else if (type[i] == -2) {
				temp = new byte[4];
				for (int j = 0; j < 4; j++)
					temp[j] = data[position + j];
				s[i] = new Float(byteTofloat(temp)).toString();
				position += 4;
			} else if (type[i] >= 0) {
				temp = new byte[type[i] * 2];
				byte[] b = new byte[2];
				char[] c = new char[type[i]];
				for (int j = 0; j < type[i] * 2; j++)
					temp[j] = data[position + j];
				for (int j = 0; j < c.length; j++) {
					b[0] = temp[j * 2];
					b[1] = temp[j * 2 + 1];
					c[j] = byteTochar(b);
				}
				s[i] = new String(c);
				position += type[i] * 2;
			}
		}
		return s;
	}

	// encode int to a byte array
	public static byte[] intTobyte(int number) {
		int temp = number;
		byte[] b = new byte[4];
		for (int i = b.length - 1; i > -1; i--) {
			b[i] = new Integer(temp & 0xff).byteValue();
			temp = temp >> 8;
		}
		return b;
	}

	// decode a byte array to a int
	public static int byteToint(byte[] b) {
		int s = 0;
		for (int i = 0; i < 3; i++) {
			if (b[i] >= 0)
				s = s + b[i];
			else
				s = s + 256 + b[i];
			s = s * 256;
		}
		if (b[3] >= 0)
			s = s + b[3];
		else
			s = s + 256 + b[3];
		return s;
	}

	public static byte[] floatTobyte(float number) {
		byte[] b;
		b = intTobyte(Float.floatToIntBits(number));
		return b;
	}

	public static float byteTofloat(byte[] b) {

		return Float.intBitsToFloat(byteToint(b));
	}

	public static byte[] shortTobyte(short number) {
		short temp = number;
		byte[] b = new byte[2];
		for (int i = b.length - 1; i > -1; i--) {
			b[i] = new Short((short) (temp & 0xff)).byteValue();
			temp = (short) (temp >> 8);
		}
		return b;
	}

	public static short byteToshort(byte[] b) {
		short s = 0;
		for (int i = 0; i < 1; i++) {
			if (b[i] >= 0)
				s = (short) (s + b[i]);
			else
				s = (short) (s + 256 + b[i]);
			s = (short) (s * 256);
		}
		if (b[1] >= 0)
			s = (short) (s + b[1]);
		else
			s = (short) (s + 256 + b[1]);
		return s;
	}

	public static byte[] charTobyte(char ch) {
		int temp = (int) ch;
		byte[] b = new byte[2];
		for (int i = b.length - 1; i > -1; i--) {
			b[i] = new Integer(temp & 0xff).byteValue();
			temp = temp >> 8;
		}
		return b;
	}

	public static char byteTochar(byte[] b) {
		int s = 0;
		if (b[0] > 0)
			s += b[0];
		else
			s += 256 + b[0];
		s *= 256;
		if (b[1] > 0)
			s += b[1];
		else
			s += 256 + b[1];
		char ch = (char) s;
		return ch;
	}

	public void show_table() {
		String database = getCurrentDatabase();
		File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator + database);
		File[] fList = ff.listFiles();
		for (int i = 0; i < fList.length; i++) {
			if (fList[i].getName().contains(".dat")) {
				String tmp = fList[i].getName();
				int pos = tmp.indexOf('.');
				System.out.println(tmp.subSequence(0, pos));
			}
		}
	}
}