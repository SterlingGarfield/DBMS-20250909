import java.io.*;
import java.util.*;

/**
 * 对分类对象进行序列化处理
 */
class Catalog implements Serializable {
	private static final long serialVersionUID = 1L;
	//// ****Fields****////

	private File thefile; // catalog file handle
	private String database_name; // database name
	private int num_of_table; // numbers of tables in this database
	private ArrayList table_name; // the name of tables in this database
	private ArrayList primary_key; // the primary key of each table
	private int[] table_length; // the length of a record in each table
	private int[] num_of_attr; // number of attributes in each table
	private int[] num_of_indexed_attr; // number of indexed attributes in each table

	ArrayList[] attr_name; // the attribute names array of each tables
	ArrayList[] offset; // the offset of each attribute in tables
	ArrayList[] attr_type; // the type of each attribute in tables
	ArrayList[] attr_length; // the length of each attribute in tables
	ArrayList[] is_unique; // whether the attributes is unique
	ArrayList[] index_name; // the index name of each indexed attribute

	public Catalog() {
	}

	public Catalog(File ff) {
		thefile = ff;
	}

	public Catalog(File ff, String dn, int nr, ArrayList rn, ArrayList pk, int[] rl, int[] na, int[] nia,
			ArrayList[] an, ArrayList[] off, ArrayList[] at, ArrayList[] al, ArrayList[] iu, ArrayList[] in) {
		thefile = ff;
		database_name = dn;
		num_of_table = nr;
		table_name = rn;
		primary_key = pk;
		table_length = rl;
		num_of_attr = na;
		num_of_indexed_attr = nia;
		attr_name = an;
		offset = off;
		attr_type = at;
		attr_length = al;
		is_unique = iu;
		index_name = in;
	}

	// **Accessors**//
	public File getFile() {
		return thefile;
	}

	public String getDatabase_name() {
		return database_name;
	}

	public int getNum_of_table() {
		return num_of_table;
	}

	public ArrayList getTable_name() {
		return table_name;
	}

	public ArrayList getPrimary_key() {
		return primary_key;
	}

	public int[] getTable_length() {
		return table_length;
	}

	public int[] getNum_of_attr() {
		return num_of_attr;
	}

	public int[] getNum_of_indexed_attr() {
		return num_of_indexed_attr;
	}

	public ArrayList[] getAttr_name() {
		return attr_name;
	}

	public ArrayList[] getOffset() {
		return offset;
	}

	public ArrayList[] getAttr_type() {
		return attr_type;
	}

	public ArrayList[] getAttr_length() {
		return attr_length;
	}

	public ArrayList[] getIs_unique() {
		return is_unique;
	}

	public ArrayList[] getIndex_name() {
		return index_name;
	}

	// **Mutators**//
	public void setFile(File ff) {
		thefile = ff;
	}

	public void setDatabase_name(String dbn) {
		database_name = dbn;
	}

	public void setNum_of_table(int n) {
		num_of_table = n;
	}

	public void setTable_name(ArrayList rn) {
		table_name = rn;
	}

	public void setPrimary_key(ArrayList pk) {
		primary_key = pk;
	}

	public void setTable_length(int[] rl) {
		table_length = rl;
	}

	public void setNum_of_attr(int[] na) {
		num_of_attr = na;
	}

	public void setNum_of_indexed_attr(int[] nia) {
		num_of_indexed_attr = nia;
	}

	public void setAttr_name(ArrayList[] an) {
		attr_name = an;
	}

	public void setOffset(ArrayList[] oo) {
		offset = oo;
	}

	public void setAttr_type(ArrayList[] at) {
		attr_type = at;
	}

	public void setAttr_length(ArrayList[] al) {
		attr_length = al;
	}

	public void setIs_unique(ArrayList[] iu) {
		is_unique = iu;
	}

	public void setIndex_name(ArrayList[] in) {
		index_name = in;
	}

	public void increaseNum_of_table() {
		num_of_table++;
	}

	public void insertTable_name(String tn) {
		table_name.add(tn);
	}

	public void insertPrimary_key(String pk) {
		primary_key.add(pk);
	}

	public void insertTable_length(int iii) {
		int[] ii = new int[table_length.length + 1];
		for (int i = 0; i < table_length.length; i++)
			ii[i] = table_length[i];
		ii[ii.length - 1] = iii;
		table_length = ii;
	}

	public void insertNum_of_attr(int iii) {
		int[] ii = new int[num_of_attr.length + 1];
		for (int i = 0; i < num_of_attr.length; i++)
			ii[i] = num_of_attr[i];
		ii[ii.length - 1] = iii;
		num_of_attr = ii;
	}

	public void insertNum_of_indexed_attr(int iii) {
		int[] ii = new int[num_of_indexed_attr.length + 1];
		for (int i = 0; i < num_of_indexed_attr.length; i++)
			ii[i] = num_of_indexed_attr[i];
		ii[ii.length - 1] = iii;
		num_of_indexed_attr = ii;
	}

	public void increaseNum_of_indexed_attr(int tbno) {
		num_of_indexed_attr[tbno]++;
	}

	public void insertAttr_name(String[] an) {
		ArrayList[] al = new ArrayList[attr_name.length + 1];
		al[al.length - 1] = new ArrayList();
		for (int i = 0; i < attr_name.length; i++)
			al[i] = attr_name[i];
		for (int i = 0; i < an.length; i++)
			al[al.length - 1].add(an[i]);
		attr_name = al;
	}

	public void insertOffset(int[] iii) {
		ArrayList[] al = new ArrayList[offset.length + 1];
		al[al.length - 1] = new ArrayList();
		for (int i = 0; i < offset.length; i++)
			al[i] = offset[i];
		for (int i = 0; i < iii.length; i++)
			al[al.length - 1].add(new Integer(iii[i]));
		offset = al;
	}

	public void insertAttr_type(int[] iii) {
		ArrayList[] al = new ArrayList[attr_type.length + 1];
		al[al.length - 1] = new ArrayList();
		for (int i = 0; i < attr_type.length; i++)
			al[i] = attr_type[i];
		for (int i = 0; i < iii.length; i++)
			al[al.length - 1].add(new Integer(iii[i]));
		attr_type = al;
	}

	public void insertAttr_length(int[] iii) {
		ArrayList[] al = new ArrayList[attr_length.length + 1];
		al[al.length - 1] = new ArrayList();
		for (int i = 0; i < attr_length.length; i++)
			al[i] = attr_length[i];
		for (int i = 0; i < iii.length; i++)
			al[al.length - 1].add(new Integer(iii[i]));
		attr_length = al;
	}

	public void insertIs_unique(boolean[] iu) {
		ArrayList[] al = new ArrayList[is_unique.length + 1];
		al[al.length - 1] = new ArrayList();
		for (int i = 0; i < is_unique.length; i++)
			al[i] = is_unique[i];
		for (int i = 0; i < iu.length; i++)
			al[al.length - 1].add(new Boolean(iu[i]));
		is_unique = al;
	}

	public void insertIndex_name(String[] ss) {
		ArrayList[] al = new ArrayList[index_name.length + 1];
		al[al.length - 1] = new ArrayList();
		for (int i = 0; i < index_name.length; i++)
			al[i] = index_name[i];
		for (int i = 0; i < ss.length; i++)
			al[al.length - 1].add(ss[i]);
		index_name = al;
	}

	public void insertIndex_name(String ss, int table_name, int attribute_no) {
		index_name[table_name].set(attribute_no, ss);
	}

	public void decreaseNum_of_table() {
		if (num_of_table == 0)
			return;
		else
			num_of_table--;
	}

	public void deleteTable_name(int table_no) {
		table_name.remove(table_no);

	}

	public void deletePrimary_key(int table_no) {
		primary_key.remove(table_no);

	}

	public void deleteTable_length(int table_no) {
		int[] ii = new int[table_length.length - 1];
		for (int i = 0; i < ii.length; i++)
			if (i < table_no)
				ii[i] = table_length[i];
			else
				ii[i] = table_length[i + 1];
		table_length = ii;

	}

	public void deleteNum_of_attr(int table_no) {
		int[] ii = new int[num_of_attr.length - 1];
		for (int i = 0; i < ii.length; i++)
			if (i < table_no)
				ii[i] = num_of_attr[i];
			else
				ii[i] = num_of_attr[i + 1];
		num_of_attr = ii;

	}

	public void deleteNum_of_indexed_attr(int table_no) {
		int[] ii = new int[num_of_indexed_attr.length - 1];
		for (int i = 0; i < ii.length; i++)
			if (i < table_no)
				ii[i] = num_of_indexed_attr[i];
			else
				ii[i] = num_of_indexed_attr[i + 1];
		num_of_indexed_attr = ii;

	}

	public void decreaseNum_of_indexed_attr(int table_no) {
		if (num_of_indexed_attr[table_no] == 0)
			return;
		else
			num_of_indexed_attr[table_no]--;
	}

	public void deleteAttr_name(int table_no) {
		ArrayList[] al = new ArrayList[attr_name.length - 1];
		for (int i = 0; i < al.length; i++)
			if (i < table_no)
				al[i] = attr_name[i];
			else
				al[i] = attr_name[i + 1];
		attr_name = al;

	}

	public void deleteOffset(int table_no) {
		ArrayList[] al = new ArrayList[offset.length - 1];
		for (int i = 0; i < al.length; i++)
			if (i < table_no)
				al[i] = offset[i];
			else
				al[i] = offset[i + 1];
		offset = al;

	}

	public void deleteAttr_type(int table_no) {
		ArrayList[] al = new ArrayList[attr_type.length - 1];
		for (int i = 0; i < al.length; i++)
			if (i < table_no)
				al[i] = attr_type[i];
			else
				al[i] = attr_type[i + 1];
		attr_type = al;

	}

	public void deleteAttr_length(int table_no) {
		ArrayList[] al = new ArrayList[attr_length.length - 1];
		for (int i = 0; i < al.length; i++)
			if (i < table_no)
				al[i] = attr_length[i];
			else
				al[i] = attr_length[i + 1];
		attr_length = al;

	}

	public void deleteIs_unique(int table_no) {
		ArrayList[] al = new ArrayList[is_unique.length - 1];
		for (int i = 0; i < al.length; i++)
			if (i < table_no)
				al[i] = is_unique[i];
			else
				al[i] = is_unique[i + 1];
		is_unique = al;

	}

	public void deleteIndex_name(int table_no) {
		ArrayList[] al = new ArrayList[index_name.length - 1];
		for (int i = 0; i < al.length; i++)
			if (i < table_no)
				al[i] = index_name[i];
			else
				al[i] = index_name[i + 1];
		index_name = al;

	}

	public void deleteIndex_name(int table_no, int attr_no) {
		index_name[table_no].set(attr_no, new String(""));
	}

	public void deleteIndex_name(String in) {
		for (int i = 0; i < index_name.length; i++)
			for (int j = 0; j < index_name[i].size(); j++)
				if (((String) (index_name[i].get(j))).equals(in))
					index_name[i].set(j, new String(""));
		return;
	}

	/**
	 * 初始化新建的数据库信息，包含数据库名，表的数量，表名数组等等
	 * 
	 * @param database
	 * @throws Exception
	 */
	public void initCatalog(String database) throws Exception {
		database_name = database;
		num_of_table = 0;
		table_name = new ArrayList();
		primary_key = new ArrayList();
		table_length = new int[0];
		num_of_attr = new int[0];
		num_of_indexed_attr = new int[0];

		attr_name = new ArrayList[0];
		offset = new ArrayList[0];
		attr_type = new ArrayList[0];
		attr_length = new ArrayList[0];
		is_unique = new ArrayList[0];
		index_name = new ArrayList[0];

	}

	public int readCatalog() throws Exception {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(thefile));
		Catalog cl = (Catalog) in.readObject();
		in.close();

		thefile = cl.getFile();
		database_name = cl.getDatabase_name();
		num_of_table = cl.getNum_of_table();
		table_name = cl.getTable_name();
		primary_key = cl.getPrimary_key();
		table_length = cl.getTable_length();
		num_of_attr = cl.getNum_of_attr();
		num_of_indexed_attr = cl.getNum_of_indexed_attr();
		attr_name = cl.getAttr_name();
		offset = cl.getOffset();
		attr_type = cl.getAttr_type();
		attr_length = cl.getAttr_length();
		is_unique = cl.getIs_unique();
		index_name = cl.getIndex_name();
		return 0;
	}

	/**
	 * 将对象写入到文件
	 */
	public int writeCatalog() throws Exception {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(thefile));
		out.writeObject(new Catalog(thefile, database_name, num_of_table, table_name, primary_key, table_length,
				num_of_attr, num_of_indexed_attr, attr_name, offset, attr_type, attr_length, is_unique, index_name));
		out.close();
		return 0;
	}

}
