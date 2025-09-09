import java.io.*;

class Record {

	String database_name;
	String table_name;
	File thefile;
	int record_length;
	int page_no;
	short slot_no; // 页偏移
	byte[] record_data;

	/**
	 * 构造函数
	 */
	public Record() {
	}

	/**
	 * 构造Record，初始化某条记录的信息
	 * 
	 * @param dbname 数据库名
	 * @param tbname 表名
	 * @param ff     文件名
	 * @param rl     记录长度
	 * @param pn     页号
	 * @param sn     页偏移
	 * @param bb     记录数据
	 */
	public Record(String dbname, String tbname, File ff, int rl, int pn, short sn, byte[] bb) {
		database_name = dbname;
		table_name = tbname;
		thefile = ff;
		record_length = rl;
		page_no = pn; // 页号
		slot_no = sn; // 页偏移
		record_data = bb;
	}

	// **Accessors**//
	public String getDatabase_name() {
		return database_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public File getFile() {
		return thefile;
	}

	public int getRecord_length() {
		return record_length;
	}

	public int getPage_no() {
		return page_no;
	}

	public short getSlot_no() {
		return slot_no;
	}

	public byte[] getRecord_data() {
		return record_data;
	}

	// **Mutators**//
	public void setDatabase_name(String dbname) {
		database_name = dbname;
	}

	public void setTable_name(String tbname) {
		table_name = tbname;
	}

	public void setFile(File ff) {
		thefile = ff;
	}

	public void setRecord_length(int rl) {
		record_length = rl;
	}

	public void setPage_no(int pn) {
		page_no = pn;
	}

	public void setSlot_no(short sn) {
		slot_no = sn;
	}

	public void setRecord_data(byte[] rd) {
		record_data = rd;
	}

	/**
	 * 将记录插入分页，再插入缓存
	 * 
	 * @param bm
	 * @return
	 * @throws Exception
	 */
	public int insertRecord(Buffer_Manager bm) throws Exception {
		File_Handle fh = bm.getThisFile_Handle(thefile);
		/**
		 * 获取第一个未满页，用来插入数据
		 */
		int freepage;
		for (freepage = 0; freepage < fh.getNum_of_page(); freepage++)
			if (!fh.getIs_page_full()[freepage])
				break;
		if (freepage == fh.getNum_of_page()) // 没有空闲页，就重新分配一个新的页面
		{
			fh.allocatePage(); // TODO: 分配一个新的页时，插入了什么数据
			freepage = fh.getNum_of_page() - 1;
		}
		setPage_no(freepage);
		// 检查该页是否在缓存中，否则将其存入缓存（一般为将新建的页存入缓存）
		if (!bm.Check_page_in_buffer(thefile, freepage))
			bm.Read_page_in(thefile, freepage); // 存入缓存

		// 从缓存读取指定页
		Page_Handle ph = bm.getThisPage_Handle(thefile, freepage);

		byte[] test_data = ph.getPage_data(); // 获取页中数据
		// System.out.println();
		int pgbfno = bm.getPage_in_buffer_no(thefile, freepage);

		setSlot_no((short) (ph.getNum_of_record()));

		insertRecord(ph); // 插入记录到指定页（倒插）

		bm.setPage_buffer(ph, pgbfno);
		bm.setPage_dirty(pgbfno, true);

		fh.increaseNum_of_record();
		if ((ph.getEnd_of_freespace() - 4 - ph.getAddr_of_record().length * 2) < record_length)
			fh.setPage_full(freepage);

		bm.setTables(fh, thefile); // 更新缓存tables里面的File_Handle
		return 0;
	}

	/**
	 * 插入记录到指定页的指定位置
	 * 
	 * @param ph
	 * @return
	 */
	// insert the record at the specified slot of page ph
	public int insertRecord(Page_Handle ph) {
		byte[] data = ph.getPage_data(); // 获取页中数据
		byte[] new_data = new byte[data.length];
		/**
		 * 对于一个空闲页，
		 */
		if (slot_no == 0) {
			// i = 4070时，开始存入record_data，那么这个为倒插，从最后一个快开始插入
			for (short i = 0; i < data.length; i++) {
				if (i < ph.getEnd_of_freespace() - record_length + 1) // 小于 空闲块长度 - 记录的长度
					new_data[i] = data[i];
				else if ((i >= ph.getEnd_of_freespace() - record_length + 1)
						&& (i < Page_Handle.PAGE_SIZE - record_length))
					new_data[i] = data[i + record_length];
				else
					new_data[i] = record_data[i - (Page_Handle.PAGE_SIZE - record_length)];
			}
			System.out.println("测试输出new_data:" + new_data);
		} else {
			// i = 4044时，进入了第二个
			for (short i = 0; i < data.length; i++) {
				if ((i < ph.getEnd_of_freespace() - record_length + 1) | (i >= ph.getAddr_of_record()[slot_no - 1]))
					new_data[i] = data[i];
				else if ((i >= ph.getEnd_of_freespace() - record_length + 1)
						&& (i < ph.getAddr_of_record()[slot_no - 1] - record_length))
					new_data[i] = data[i + record_length];
				else
					new_data[i] = record_data[i - (ph.getAddr_of_record()[slot_no - 1] - record_length)];
			}
		}

		ph.setPage_data(new_data); // 将new_data存回
		ph.increaseNum_of_record();
		ph.setEnd_of_freespace((short) (ph.getEnd_of_freespace() - record_length));
		ph.insertAddr(slot_no, record_length);

		return 0;
	}

	public int deleteRecord(Buffer_Manager bm) throws Exception {
		File_Handle fh = bm.getThisFile_Handle(thefile);
		if (!bm.Check_page_in_buffer(thefile, page_no))
			bm.Read_page_in(thefile, page_no);
		Page_Handle ph = bm.getThisPage_Handle(thefile, page_no);
		int pgbfno = bm.getPage_in_buffer_no(thefile, page_no);

		deleteRecord(ph); // 从页里面删除指定记录

		bm.setPage_buffer(ph, pgbfno);
		bm.setPage_dirty(pgbfno, true);

		fh.decreaseNum_of_record();
		bm.setTables(fh, thefile);
		return 0;
	}

	public int deleteRecord(Page_Handle ph) throws Exception {

		byte[] data = ph.getPage_data();
		byte[] new_data = new byte[data.length];
		// 这个存储位置看得头皮发麻
		for (short i = 0; i < new_data.length; i++) {
			// i 小于 【页偏移 *2 + 4】？？？ 并且 大于【指定记录的地址 + 记录长度】（即在该记录之后的内容）
			if ((i < 4 + slot_no * 2) | (i > ph.getAddr_of_record()[slot_no] + record_length - 1))
				new_data[i] = data[i];
			else if ((i > ph.getEnd_of_freespace() + record_length)
					&& (i <= ph.getAddr_of_record()[slot_no] + record_length - 1))
				new_data[i] = data[i - record_length]; // 往前挪一个记录的长度
			else if ((i >= 4 + slot_no * 2) && (i < 4 + (ph.getAddr_of_record().length - 1) * 2))
				new_data[i] = data[i + 2]; // 往后挪两个
			else
				new_data[i] = 0; // slot_no = 0, i = 5，即开始执行该语句
		}

		for (short i = 0; i < ph.getAddr_of_record().length - slot_no - 1; i++) {
			byte[] b = new byte[2];
			short ss;
			b[0] = new_data[4 + (slot_no + i) * 2];
			b[1] = new_data[4 + (slot_no + i) * 2 + 1];
			ss = API.byteToshort(b);
			ss = (short) (ss + record_length); // TODO：
			b = API.shortTobyte(ss);
			new_data[4 + (slot_no + i) * 2] = b[0];
			new_data[4 + (slot_no + i) * 2] = b[1];
		}

		ph.setPage_data(new_data);
		ph.decreaseNum_of_record();
		ph.setEnd_of_freespace((short) (ph.getEnd_of_freespace() + record_length));
		ph.deleteAddr(slot_no, record_length);
		return 0;

	}

	public int getRecord(Page_Handle ph) {
		byte[] bb = new byte[record_length];

		for (short i = 0; i < record_length; i++)
			bb[i] = ph.getPage_data()[ph.getAddr_of_record()[slot_no] + i];

		record_data = bb;
		return 0;

	}

}