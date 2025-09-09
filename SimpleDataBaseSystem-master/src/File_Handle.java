import java.io.*;

/**
 * 自定义Serializable的使用，实现对象的序列化处理， 将对象写入硬盘，要用的时候再读取到内存
 */

class File_Handle implements Serializable {
	private static final long serialVersionUID = 1L;
	//// ****Fields****////
	public static final int FILE_HEAD_SIZE = 5120; // the size of file head of each file
	private File thefile; // the file information
	private String database_name; // the database to which this file belongs
	private String table_name; // the name of table which stores in this file
	private int record_length; // the length of record stores in this file
	private int num_of_page; // the number of pages in this file
	private int num_of_record; // the number of records in this file
	private boolean[] is_page_full; // if the page in this file is full

	//// ****Methods****////
	// **constructors**//
	public File_Handle() {
	}

	public File_Handle(File ff) {
		thefile = ff;
	}

	public File_Handle(File ff, String dbname, String tbname, int rl, int np, int nr, boolean[] ispagefull) {
		thefile = ff;
		database_name = dbname;
		table_name = tbname;
		record_length = rl;
		num_of_page = np;
		num_of_record = nr;
		is_page_full = ispagefull;
	}

	// **Accessors**//
	public File getFile() {
		return thefile;
	}

	public String getDatabase_name() {
		return database_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public int getRecord_length() {
		return record_length;
	}

	public int getNum_of_page() {
		return num_of_page;
	}

	public int getNum_of_record() {
		return num_of_record;
	}

	public boolean[] getIs_page_full() {
		return is_page_full;
	}

	public boolean getPage_full(int pgno) {
		return is_page_full[pgno];
	}

	// **Mutators**//
	public void setFile(File ff) {
		thefile = ff;
	}

	public void setDatabase_name(String dbname) {
		database_name = dbname;
	}

	public void setTable_name(String tbname) {
		table_name = tbname;
	}

	public void setRecord_length(int rl) {
		record_length = rl;
	}

	public void setNum_of_page(int num) {
		num_of_page = num;
	}

	public void setNum_of_record(int num) {
		num_of_record = num;
	}

	public void setIs_page_full(boolean[] ipf) {
		is_page_full = ipf;
	}

	public void increaseNum_of_page() {
		num_of_page++;
	}

	public void decreaseNum_of_page() {
		if (num_of_page == 0)
			return;
		else
			num_of_page--;
	}

	public void increaseNum_of_record() {
		num_of_record++;
	}

	public void decreaseNum_of_record() {
		if (num_of_record == 0)
			return;
		else
			num_of_record--;
	}

	public void setPage_full(int pgno) {
		is_page_full[pgno] = true;
	}

	public void setPage_notfull(int pgno) {
		is_page_full[pgno] = false;
	}

	public void initFile_Handle(String database, String tn, int rl) {

		database_name = database;
		table_name = tn;
		record_length = rl;
		num_of_page = 0;
		num_of_record = 0;
		is_page_full = new boolean[0];

	}

	public int getFreePageNO() throws Exception {
		if ((is_page_full.length == 0) | (is_page_full == null)) // 没有分配任何一页
			return allocatePage().getPage_no();
		else {
			int i;// 当前文件中未写满的页的索引
			for (i = 0; i < is_page_full.length; i++)
				if (is_page_full[i] == false)
					break;
			if (i == is_page_full.length) // 没有可使用的页
				return allocatePage().getPage_no();
			return i;
		}

	}

	public void insertPage_full(boolean full) {
		boolean[] new_full = new boolean[is_page_full.length + 1];
		for (int i = 0; i < is_page_full.length; i++)
			new_full[i] = is_page_full[i];
		new_full[new_full.length - 1] = full; // 等价于 new_full[is_page_full.length] = full;
		is_page_full = new_full;
	}

	public void deletePage_full(int pgno) {
		boolean[] new_full = new boolean[is_page_full.length - 1];
		for (int i = 0; i < new_full.length; i++)
			new_full[i] = is_page_full[i];
		new_full[pgno] = is_page_full[is_page_full.length - 1];
		is_page_full = new_full;
	}

	// **Functions**//
	// write File_Handle to the file as the file header
	public void writeFile() throws Exception {
		RandomAccessFile out = new RandomAccessFile(thefile, "rw");
		out.seek(0);
		out.writeInt(database_name.length());
		out.writeChars(database_name);
		out.writeInt(table_name.length());
		out.writeChars(table_name);
		out.writeInt(record_length);
		out.writeInt(num_of_page);
		out.writeInt(num_of_record);
		for (int i = 0; i < num_of_page; i++)
			out.writeBoolean(is_page_full[i]);
		out.close();
	}

	// read the File_Handle in the file header to buffer
	public void readFile() throws Exception {
		RandomAccessFile in = new RandomAccessFile(thefile, "r");
		in.seek(0);
		int ll = in.readInt();
		char[] cc = new char[ll];
		for (int i = 0; i < ll; i++)
			cc[i] = in.readChar();
		database_name = new String(cc);
		ll = in.readInt();
		cc = new char[ll];
		for (int i = 0; i < ll; i++)
			cc[i] = in.readChar();
		table_name = new String(cc);
		record_length = in.readInt();
		num_of_page = in.readInt();
		num_of_record = in.readInt();
		is_page_full = new boolean[num_of_page];
		for (int i = 0; i < num_of_page; i++)
			is_page_full[i] = in.readBoolean();
		in.close();

	}

	// create the file
	public void createFile(File ff) throws Exception {
		ff.createNewFile();
	}

	// delete the file
	public void deleteFile(File ff) throws Exception {
		ff.delete();
	}

	/**
	 * 分配一个新的页
	 * 
	 * @return
	 * @throws Exception
	 */
	public Page_Handle allocatePage() throws Exception {
		Page_Handle pp = new Page_Handle(num_of_page, thefile, table_name, record_length, (short) 0,
				(short) (Page_Handle.PAGE_SIZE - 1), new short[0], new byte[Page_Handle.PAGE_SIZE]);
		pp.writeData();
		pp.writePageHead();
		num_of_page++;
		insertPage_full(false);
		return pp;
	}

	public int disposePage(int pgno) throws Exception {
		Page_Handle pp = new Page_Handle(num_of_page - 1, thefile);

		pp.readPageHead();
		pp.readData();

		pp.setPage_no(pgno);
		pp.writeData();
		pp.writePageHead();

		num_of_page--;

		RandomAccessFile ff = new RandomAccessFile(pp.getFile(), "rw");
		ff.setLength(FILE_HEAD_SIZE + Page_Handle.PAGE_SIZE * num_of_page);

		deletePage_full(pgno);
		return 0;
	}

}