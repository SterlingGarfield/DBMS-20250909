import java.io.*;

/**
 * 分页
 */
class Page_Handle
{
	////****Fields****////
	public static final int PAGE_SIZE = 4096;  //the size of each page
	private int page_no;                       //page number
	private File thefile;                      //file which this page belongs to
	private String table_name;                 //name of which table the page belongs to
	private int record_length;                 //the length of record in this page
	private short num_of_record;               //numbers of records in this page
	private short end_of_freespace;            //the end address of freespace
	private short[] addr_of_record;            //the address of every record in this page
	private byte[] page_data;                  //data of this page
	

	////****Methods****////
	//**constructors**//
	public Page_Handle() {}
	
	public Page_Handle(int pageno, File ff)
	{
		page_no = pageno;
		thefile = ff;
		page_data = new byte[PAGE_SIZE];
	}
	
	public Page_Handle(int pgno, File ff, String tbname, int rl, short numrd, short endfs, short[] add)
	{
		page_no = pgno;
		thefile = ff;
		table_name = tbname;
		record_length = rl;
		num_of_record = numrd;
		end_of_freespace = endfs;
		addr_of_record = add;
		page_data = new byte[PAGE_SIZE];
	}

	/**
	 * 初始化Page_Handle对象
	 * @param pgno
	 * @param ff
	 * @param tbname
	 * @param rl
	 * @param numrd
	 * @param endfs
	 * @param add
	 * @param bb
	 */
	public Page_Handle(int pgno, File ff, String tbname, int rl, short numrd, short endfs, short[] add, byte[] bb)
	{
		page_no = pgno;
		thefile = ff;
		table_name = tbname;
		record_length = rl;
		num_of_record = numrd;
		end_of_freespace = endfs;
		addr_of_record = add;
		page_data = bb;
	}
	
	//**Accessors**//
	public int getPage_no() {return page_no;}
	public File getFile() {return thefile;}
	public String getTable_name() {return table_name;}
	public short getNum_of_record() {return num_of_record;}
	public short getEnd_of_freespace() {return end_of_freespace;}
	public short[] getAddr_of_record() {return addr_of_record;}
	public byte[] getPage_data() {return page_data;}
	
	//**Mutators**//
	public void setPage_no(int n) {page_no = n;}
	public void setFile(File ff) {thefile = ff;}
	public void setTable_name(String tn) {table_name = tn;}
	public void setNum_of_record(short n) {num_of_record = n;}
	public void setEnd_of_freespace(short n) {end_of_freespace = n;}
	public void setAddr_of_record(short[] n) {addr_of_record = n;}
	public void setPage_data(byte[] bb) {page_data = bb;}
	
	public void increaseNum_of_record() {num_of_record++;}
	
	public void decreaseNum_of_record()
	{
		if(num_of_record == 0)
			return;
		else
			num_of_record--;
		return;
	}
	
	//insert an address which valued add into the addr_of_record array
	public void insertAddr(short add)
	{
		short[] new_addr = new short[addr_of_record.length + 1];
		for(short i = 0; i < addr_of_record.length; i++)
			new_addr[i] = addr_of_record[i];
		new_addr[addr_of_record.length] = add;
		addr_of_record = new_addr;
	}
	
	public void insertAddr(short slno, int rl)
	{
		short[] new_addr = new short[addr_of_record.length + 1];
		if(slno == 0)
			for(short i = 0; i < new_addr.length; i++)
			{
				if(i < slno)
					new_addr[i] = addr_of_record[i];
				else if(i == slno)
					new_addr[i] = (short)(Page_Handle.PAGE_SIZE - rl);
				else
					new_addr[i] = (short)(addr_of_record[i - 1] - rl);
			}
		else
			for(short i = 0; i < new_addr.length; i++)
			{
				if(i < slno)
					new_addr[i] = addr_of_record[i];
				else if(i == slno)
					new_addr[i] = (short)(addr_of_record[i - 1] - rl);
				else
					new_addr[i] = (short)(addr_of_record[i - 1] - rl);
			}
		addr_of_record = new_addr;
	}
	
	//delete an address which slot_no sepecified to 
	public void deleteAddr(short slot_no, int rl)
	{
		short[] new_addr = new short[addr_of_record.length - 1];
		for(short i = 0; i < new_addr.length; i ++)
		{
			if(i < slot_no)
				new_addr[i] = addr_of_record[i];
			else
				new_addr[i] = (short)(addr_of_record[i + 1] + rl);
		}
		addr_of_record = new_addr;
				
	}
	
	//**Functions**//
	//init a new page
	public int initPage() throws Exception
	{
		setNum_of_record((short)0);
		setEnd_of_freespace((short)(PAGE_SIZE - 1));
		setAddr_of_record(new short[0]);
		setPage_data(new byte[PAGE_SIZE]);
		this.writeData();
		this.writePageHead();
		return 0;
	}
	
	//read the data from disk to buffer
	public int readData() throws Exception
	{
		RandomAccessFile in = new RandomAccessFile(thefile, "r");
		in.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no);
		in.read(page_data, 0, page_data.length);
	    in.close();
		return 0;
		
	}
	
	//write the data back to the disk
	public int writeData() throws Exception
	{
		RandomAccessFile out = new RandomAccessFile(thefile, "rw");
		out.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no);
		out.write(page_data, 0, page_data.length);
		out.close();
		writePageHead();
		return 0;
	}
	
	//read the head information of page
	public int readPageHead() throws Exception
	{
		RandomAccessFile in = new RandomAccessFile(thefile, "r");
		in.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no);
		num_of_record = in.readShort();
		in.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no + 2);
		end_of_freespace = in.readShort();
		in.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no + 4);
		addr_of_record = new short[num_of_record];
		for(short i = 0; i < num_of_record; i++)
		{
			addr_of_record[i] = in.readShort();
			in.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no + 4 + 2 * (i + 1));
		}
		in.close();
		return 0;
	}
	
	//write the head information of page into file
	public int writePageHead() throws Exception
	{
		RandomAccessFile out = new RandomAccessFile(thefile, "rw");
		out.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no);
		out.writeShort(num_of_record);
		out.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no + 2);
		out.writeShort(end_of_freespace);
		out.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no + 4);
		for(int i = 0; i < num_of_record; i++)
		{
			out.writeShort(addr_of_record[i]);
			out.seek(File_Handle.FILE_HEAD_SIZE + PAGE_SIZE * page_no + 4 + 2 * (i + 1));
		}
		out.close();
		return 0;
	}
	
	//return true if the page is full
	public boolean is_full()
	{
		return ((end_of_freespace - (4 + num_of_record * 2) + 1) < record_length);
	}
	
}













