import java.io.*;

/**
 * 记录管理器,管理数据记录
 */
class Record_Manager {
    /**
     * 插入数据到dat文件
     *
     * @param table_name 表名
     * @param data       数据
     * @return
     * @throws Exception
     */
    public int insertRecord(String table_name, byte[] data) throws Exception {
        File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator
                + HNUSQL.api.getCurrentDatabase() + File.separator + table_name + ".dat");
        Record rr = new Record(HNUSQL.api.getCurrentDatabase(), table_name, ff, data.length, 0, (short) 0, data);
        rr.insertRecord(HNUSQL.bm); // 将文件插入缓存，创建表的时候，将表的信息加入了缓存对象的tables数组里面，插入数据的时候，将缓存插入文件

        return 0;
    }

    /**
     * 删除记录
     * 
     * @param table_name
     * @param record_no
     * @param record_length
     * @return
     * @throws Exception
     */
    public int deleteRecord(String table_name, int record_no, int record_length) throws Exception {
        File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator
                + HNUSQL.api.getCurrentDatabase() + File.separator + table_name + ".dat");
        int Num_of_page = HNUSQL.bm.getThisFile_Handle(ff).getNum_of_page();
        int Num_of_record = HNUSQL.bm.getThisFile_Handle(ff).getNum_of_record();
        int[] Num_of_record_each_page = new int[Num_of_page];
        int temp = record_no;
        byte[] data = new byte[record_length];

        for (int i = 0; i < Num_of_page; i++) {
            if (!HNUSQL.bm.Check_page_in_buffer(ff, i))
                HNUSQL.bm.Read_page_in(ff, i);
            Num_of_record_each_page[i] = HNUSQL.bm.getThisPage_Handle(ff, i).getNum_of_record();
        }

        int j;
        for (j = 0; j < Num_of_page; j++) {
            if (temp <= Num_of_record_each_page[j] - 1)
                break;
            else
                temp -= Num_of_record_each_page[j];
        }
        if (!HNUSQL.bm.Check_page_in_buffer(ff, j))
            HNUSQL.bm.Read_page_in(ff, j);

        Record rc = new Record(HNUSQL.api.getCurrentDatabase(), table_name, ff, record_length, j, (short) (temp), data);
        rc.deleteRecord(HNUSQL.bm);

        return 0;
    }

    /**
     * 获取记录数
     * 
     * @param table_name
     * @return
     */
    public int getNum_of_Record(String table_name) {
        File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator
                + HNUSQL.api.getCurrentDatabase() + File.separator + table_name + ".dat");
        return HNUSQL.bm.getThisFile_Handle(ff).getNum_of_record();
    }

    /**
     * 查询数据
     *
     * @param table_name    表名
     * @param record_no     记录序号
     * @param record_length
     * @return
     * @throws Exception
     */
    public byte[] getRecord(String table_name, int record_no, int record_length) throws Exception {
        File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator
                + HNUSQL.api.getCurrentDatabase() + File.separator + table_name + ".dat");

        int Num_of_page = HNUSQL.bm.getThisFile_Handle(ff).getNum_of_page(); // 已有页的数量

        int temp = 0;
        int Num_of_record_each_page = Page_Handle.PAGE_SIZE / record_length; // 每页含有多少条记录
        boolean[] bb = new boolean[Num_of_page];
        bb = HNUSQL.bm.getThisFile_Handle(ff).getIs_page_full();
        int j;
        byte[] data = new byte[record_length];
        /**
         * 统计记录的数量 temp：页偏移，所指定记录在该页的位置
         */
        for (j = 0; j < Num_of_page; j++) {
            if (bb[j])
                temp = temp + Num_of_record_each_page;
            else {
                if (!HNUSQL.bm.Check_page_in_buffer(ff, j))
                    HNUSQL.bm.Read_page_in(ff, j);
                temp = temp + HNUSQL.bm.getThisPage_Handle(ff, j).getNum_of_record();
            }

            if (temp > record_no)
                break;
        }

        if (!HNUSQL.bm.Check_page_in_buffer(ff, j))
            HNUSQL.bm.Read_page_in(ff, j);

        temp = record_no - temp + HNUSQL.bm.getThisPage_Handle(ff, j).getNum_of_record();

        Record rc = new Record(HNUSQL.api.getCurrentDatabase(), table_name, ff, record_length, j, (short) (temp), data);
        rc.getRecord(HNUSQL.bm.getThisPage_Handle(ff, j));
        data = rc.getRecord_data();
        return data;

    }

    /**
     * 创建表dat文件
     *
     * @param table_name
     * @return
     * @throws Exception
     */
    public int createTable(String table_name) throws Exception {

        File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator
                + HNUSQL.api.getCurrentDatabase() + File.separator + table_name + ".dat");
        ff.createNewFile();
        File_Handle fh = new File_Handle(ff);
        fh.initFile_Handle(HNUSQL.api.getCurrentDatabase(), table_name, HNUSQL.cm.getRecord_length(table_name));
        HNUSQL.bm.insertFile_Handle(fh);
        fh.writeFile();
        return 0;
    }

    /**
     * 删除表
     * 
     * @param table_name
     * @return
     */
    public int dropTable(String table_name) {
        File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator
                + HNUSQL.api.getCurrentDatabase() + File.separator + table_name + ".dat");
        HNUSQL.bm.dropTable(ff);
        ff.delete();
        return 0;
    }

    /**
     * 删除数据库
     * 
     * @param database_name
     * @return
     */
    public int dropDatabase(String database_name) {
        HNUSQL.bm.dropDatabase(database_name);
        File dir = new File(HNUSQL.MINISQL_PATH + File.separator + "data" + File.separator + database_name);
        String[] filenames = dir.list();

        for (int i = 0; i < filenames.length; i++) {
            if (filenames[i].endsWith(".dat")) {
                File ff = new File(dir.getAbsolutePath() + File.separator + filenames[i]);
                ff.delete();
            }
        }
        return 0;
    }
}