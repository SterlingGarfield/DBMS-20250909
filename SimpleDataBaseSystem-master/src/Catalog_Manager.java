import java.io.*;

/**
 * 分类管理器，提供包括库、表的元信息等
 */
class Catalog_Manager
{
	private Catalog cat;
	
	public Catalog_Manager() {}

	
	//create a catalog file named database_name.cat
	public int createDatabase(String database_name) throws Exception
	{
		File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator + database_name + File.separator + database_name + ".cat");
		ff.createNewFile();
		Catalog ct = new Catalog(ff);
		ct.initCatalog(database_name); // 初始化数据库信息
		ct.writeCatalog();  // 将信息（对象）写入文件
		
		return 0;
	}
	
	public int UseDatabase(String database_name) throws Exception
	{
		if(cat != null)
			cat.writeCatalog();
		File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator + database_name + File.separator + database_name + ".cat");
		cat = new Catalog(ff);
		cat.readCatalog();
		return 0;
	}
	
	public boolean checkTable(String table_name)
	{
		if(cat.getTable_name() == null)
			return false;
		for(int i = 0; i < cat.getTable_name().size(); i++)
			if(((String)(cat.getTable_name().get(i))).equals(table_name))
				return true;
		return false;
	}
	
	public boolean checkAttribute(String table_name, String attribute_name)
	{
		return attribute_name.equals((String)(cat.getAttr_name()[getTableNO(table_name)].get(getAttributeNO(table_name, attribute_name))));
	}
	public boolean checkAttributeNum(String table_name, int i)
	{
		return (i == cat.getAttr_name()[getTableNO(table_name)].size());
	}
	public boolean checkAttributeType(String table_name, int attr_no, int type)
	{
		int table_no = getTableNO(table_name);
		if(((Integer)(cat.getAttr_type()[table_no].get(attr_no))).intValue() < 0)
			return (type == ((Integer)(cat.getAttr_type()[table_no].get(attr_no))).intValue());
		else
			return (type <= ((Integer)(cat.getAttr_type()[table_no].get(attr_no))).intValue());
	}
	
	public boolean checkIndex_name(String index_name)
	{
		return false;
	}
	public boolean checkIndex(String table_name, String attribute_name)
	{
		return false;
	}
	public boolean checkUnique(String table_name, String attribute_name)
	{
		return false;
	}
	public int getRecord_length(String table_name)
	{
		return cat.getTable_length()[getTableNO(table_name)];
	}
	public int getNum_of_attr(String table_name) {return  cat.getNum_of_attr()[getTableNO(table_name)];}
	public int getAttributeType(String table_name, String attribute_name)
	{
		int i = getTableNO(table_name);
		int j = getAttributeNO(table_name, attribute_name);
		
		return ((Integer)(cat.getAttr_type()[i].get(j))).intValue();
	}
	
	public int[] getAttributeTypes(String table_name)
	{
		int[] i = new int[cat.getNum_of_attr()[getTableNO(table_name)]];
		for(int j = 0; j < (cat.getAttr_type()[getTableNO(table_name)]).size(); j++)
			i[j] = ((Integer)((cat.getAttr_type()[getTableNO(table_name)]).get(j))).intValue();
		return i;
	}
	
	public int getAttributeLength(String table_name, String attribute_name)
	{
		int i = getTableNO(table_name);
		int j = getAttributeNO(table_name, attribute_name);
		
		return ((Integer)(cat.getAttr_length()[i].get(j))).intValue();
	}
	
	public int getTableNO(String table_name)
	{
		int i;
		for(i = 0; i < cat.getNum_of_table(); i++)
			if(((String)(cat.getTable_name().get(i))).equals(table_name))
				break;
		return i;
	}
	
	public int getAttributeNO(String table_name, String attribute_name)
	{
		int i, j;
		i = getTableNO(table_name);
		for(j = 0; j < cat.getNum_of_attr()[i]; j++)
			if(((String)(cat.getAttr_name()[i].get(j))).equals(attribute_name))
					break;
		return j;
	}
	
	public int getAttributeOffset(String table_name, String attribute_name)
	{
		int i,j;
		i = getTableNO(table_name);
		j = getAttributeNO(table_name, attribute_name);
		return ((Integer)(cat.getOffset()[i].get(j))).intValue();
	}

    /**
     * 将新建表的信息插入数据库文件（cat）中
     * @param table_name
     * @param attr_name
     * @param attr_type
     * @param is_unique
     * @param primary_key
     * @return
     */
	public int createTable(String table_name, String[] attr_name, int[] attr_type, boolean[] is_unique, String primary_key)
	{
		cat.insertTable_name(table_name);
		cat.insertAttr_name(attr_name);
		cat.insertAttr_type(attr_type);
		cat.insertIs_unique(is_unique);
		cat.insertPrimary_key(primary_key);
		cat.insertNum_of_attr(attr_name.length);
		cat.insertNum_of_indexed_attr(1);
		
		int table_length = 0;
		int[] offset = new int[attr_name.length];
		int[] attr_length = new int[attr_name.length];
		
		for(int i = 0; i < attr_name.length; i++)
		{
			if((attr_type[i] == -1) | (attr_type[i] == -2))
				attr_length[i] = 4;
			else if(attr_type[i] > 0)
				attr_length[i] = attr_type[i] * 2;
			if(i == 0)
				offset[i] = 0;
			else
				offset[i] = table_length;
			table_length += attr_length[i];		
			
		}
		
		cat.insertTable_length(table_length);
		cat.insertOffset(offset);
		cat.insertAttr_length(attr_length);
		
		String[] index_name = new String[attr_name.length];
		int j;
		for(j = 0; j < attr_name.length; j++)
			if(attr_name[j].equals(primary_key))
				break;
		index_name[j] = "idx" + primary_key;
		
		cat.insertIndex_name(index_name);

		cat.increaseNum_of_table();
		return 0;
	}
	
	public int createIndex(String index_name, String table_name, String attr_name)
	{
		int i = getTableNO(table_name);
		int j = getAttributeNO(table_name, attr_name);
		cat.increaseNum_of_indexed_attr(i);
		cat.insertIndex_name(index_name, i, j);
		return 0;
	}
	
	public int dropDatabase(String database_name)
	{
		File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator + database_name + File.separator + database_name + ".cat");
		ff.delete();
		return 0;
	}
	
	public int dropTable(String table_name)
	{
		int i = getTableNO(table_name);
		cat.deleteIndex_name(i);
		cat.deleteIs_unique(i);
		cat.deleteAttr_length(i);
		cat.deleteAttr_type(i);
		cat.deleteOffset(i);
		cat.deleteAttr_name(i);
		cat.deleteNum_of_indexed_attr(i);
		cat.deleteNum_of_attr(i);
		cat.deleteTable_length(i);
		cat.deletePrimary_key(i);
		cat.deleteTable_name(i);
		cat.decreaseNum_of_table();
		return 0;
	}
	public int dropIndex(String index_name)
	{
		cat.deleteIndex_name(index_name);
		return 0;
	}
	
	public String[] getIndex_name(String table_name)
	{
		return new String[0];
	}
	
	public int quit() throws Exception
	{
		if(cat != null)
			cat.writeCatalog();
		return 0;
	}

	
}