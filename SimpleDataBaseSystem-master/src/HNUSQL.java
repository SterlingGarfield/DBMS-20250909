import java.io.*;

class HNUSQL {
    public static Buffer_Manager bm = new Buffer_Manager();
    public static Catalog_Manager cm = new Catalog_Manager();
    public static Record_Manager rm = new Record_Manager();
    public static Index_Manager im = new Index_Manager();
    public static API api = new API();

    public static final String MINISQL_PATH = (new File((new File("HNUSQL.java")).getAbsolutePath())).getParentFile()
            .getAbsolutePath();

    public static void main(String[] args) throws Exception {

        File dir = new File(MINISQL_PATH + File.separator + "data");
        if (!dir.exists())
            dir.mkdir();
        Interpreter inter = new Interpreter();
        inter.begin();
    }
}