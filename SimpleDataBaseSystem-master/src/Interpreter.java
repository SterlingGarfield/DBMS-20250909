import java.io.*;
import java.util.*;
import java.util.regex.*;

/**
 * SQL语句解释器 1 类定义了命令的格式，使用正则表达式来匹配命令，没有词法分析的模块。 2. 定义了整个数据库的执行逻辑
 */
class Interpreter {
    private String query;
    private Date queryTime;
    private long millisecond;
    private static String create_database_pattern;
    private static String create_table_pattern;
    private static String create_index_pattern;
    private static String use_pattern;
    private static String drop_database_pattern;
    private static String drop_table_pattern;
    private static String drop_index_pattern;
    private static String insert_pattern;
    private static String delete_pattern;
    private static String update_pattern;
    private static String select_pattern;
    private static String execfile_pattern;
    private static String show_database_pattern;
    private static String show_table_pattern;

    public Interpreter() { // 定义正则表达式
        query = "";
        create_database_pattern = "create database [^\\s]+";
        create_table_pattern = "create table [^\\s]+.*\\(.*\\)";
        create_index_pattern = "create index [^\\s]+ on [^\\s]+.*\\([\\s]*[^\\s]+[\\s]*\\)";
        use_pattern = "use [^\\s]+";
        drop_database_pattern = "drop database [^\\s]+";
        drop_table_pattern = "drop table [^\\s]+";
        drop_index_pattern = "drop index [^\\s]+";
        insert_pattern = "insert into [^\\s]+ values[\\s]*\\(.+\\)";
        delete_pattern = "delete from [^\\s]+.*";
        update_pattern = "update [^\\s]+ set [^\\s]+.* where .*";
        select_pattern = "select [^\\s].* from [^\\s]+.*";
        execfile_pattern = "execfile .*";
        show_database_pattern = "show databases";
        show_table_pattern = "show tables";
    }

    public void begin() throws Exception {
        // BufferedReader: 缓冲字符输入流，为了读入多行命令
        BufferedReader rd = new BufferedReader(new InputStreamReader(System.in));
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(System.out), true);
        boolean finish = true;
        System.out.println("**************************************************欢迎来到湖南大学简易MYSQL数据库系统**************************************************************");
        System.out.println("                作者：杜敏-F1810W0204                                        ");
        System.out.println("                邮箱：2712220318@qq.com                                        ");
        System.out.println("                项目地址：https://gitee.com/zscdumin/SimpleDataBaseSystem      ");
        System.out.println("                【注意】：所有命令（除quit和exit外）都要以分号(;)结尾。" +
                                            "一个语句可以分几行输入，系统以分号判断语句是否结束。");
        System.out.println("**********************************************************************结束**********************************************************************");
        while ((!query.equalsIgnoreCase("exit")) && (!query.equalsIgnoreCase("quit"))) {
            if (finish)// 判断输入是否结束 以finish作为标志
            {
                System.out.print("mysql>");
                query = rd.readLine();
                if (query.endsWith(";")) {
                    finish = true;
                    interpret(query.toLowerCase());
                } else {
                    finish = false;
                }
            } else {
                System.out.print("	>");
                String temp = rd.readLine();
                if (temp == ";")
                    query = query + temp;
                else
                    query = query + " " + temp;
                if (query.endsWith(";"))// 输入结束
                {
                    finish = true;
                    interpret(query.toLowerCase());// 解析输入语句
                    query = "";
                } else {
                    finish = false;
                }
            }
        }
        HNUSQL.bm.quit();// 退出Buffer_Manager
        HNUSQL.cm.quit();// 退出Catalog_Manager
    }

    /**
     * 构造函数，所有输入的命令 ss 都调用该构造函数，该函数为ss匹配相关的操作
     *
     * @param ss
     * @return
     * @throws Exception
     */
    public int interpret(String ss) throws Exception {
        ss = clean(ss);
        // 接下来为ss匹配合适的操作，如果没有合适的匹配 那么就显示语法错误
        if (ss.matches(create_database_pattern)) {
            create_database_interpret(ss);
            return 0;
        }
        if (ss.matches(create_table_pattern)) {
            create_table_interpret(ss);
            return 0;
        }
        if (ss.matches(create_index_pattern)) {
            create_index_interpret(ss);
            return 0;
        }
        if (ss.matches(use_pattern)) {
            use_interpret(ss);
            return 0;
        }
        if (ss.matches(drop_database_pattern)) {
            drop_database_interpret(ss);
            return 0;
        }
        if (ss.matches(drop_table_pattern)) {
            drop_table_interpret(ss);
            return 0;
        }
        if (ss.matches(drop_index_pattern)) {
            drop_index_interpret(ss);
            return 0;
        }
        if (ss.matches(insert_pattern)) {
            insert_interpret(ss);
            return 0;
        }
        if (ss.matches(delete_pattern)) {
            delete_interpret(ss);
            return 0;
        }
        if (ss.matches(update_pattern)) {
            update_interpret(ss);
            return 0;
        }
        if (ss.matches(select_pattern)) {
            select_interpret(ss);
            return 0;
        }
        if (ss.matches(execfile_pattern)) {
            execfile_interpret(ss);
            return 0;
        }
        if (ss.matches(show_database_pattern)) {
            show_database_interpret();
            return 0;
        }

        if (ss.matches(show_table_pattern)) {
            show_table_interpret();
            return 0;
        }

        System.out.println("*****Syntax Error*****");
        return 0;
    }

    /**
     * 清除字符串前端和末尾的空格和分号，将字符串中间的连续空格一律压缩成一个空格(引号内的除外)
     *
     * @param s
     * @return
     */
    public static String clean(String s) {
        char[] c = s.toCharArray();
        // 清除字符前的空格 回车符 tab符 以及换行符
        if (c[0] == ' ' | c[0] == '\r' | c[0] == '\n' | c[0] == '\t') {
            int i;
            for (i = 0; i < c.length; i++)
                if (c[i] != ' ' && c[i] != '\r' && c[i] != '\n' && c[i] != '\t')
                    break;
            c = s.substring(i).toCharArray();
            s = new String(c);
        }

        c = s.toCharArray();
        int count = 0;
        int j, k;
        for (j = 0; j < c.length; j++) {
            if (c[j] == '\"' | c[j] == '\'')
                count++;
        }
        count = count / 2;

        String[] yes = new String[count];
        String[] no = new String[count + 1];

        j = 0;
        k = -1;
        for (int i = 0; i < yes.length; i++) {
            for (j = k + 1; j < c.length; j++)
                if (c[j] == '\"' | c[j] == '\'')
                    break;
            no[i] = s.substring(k + 1, j);
            for (k = j + 1; k < c.length; k++)
                if (c[k] == '\"' | c[k] == '\'')
                    break;
            if (k == (c.length - 1))
                yes[i] = s.substring(j);
            else
                yes[i] = s.substring(j, k + 1);
        }
        if (c[c.length - 1] == '\"' | c[c.length - 1] == '\'')
            no[no.length - 1] = "";
        else
            no[no.length - 1] = s.substring(k + 1);

        for (int i = 0; i < no.length; i++) {
            String patternStr = "\\s+";
            String replaceStr = " ";
            Pattern pattern = Pattern.compile(patternStr);
            Matcher matcher = pattern.matcher(no[i]);
            no[i] = matcher.replaceAll(replaceStr);
        }

        s = "";
        for (int i = 0; i < yes.length; i++) {
            s += no[i];
            s += yes[i];
        }
        s += no[no.length - 1];

        // 清除末尾的分号 以及空格 回车符 tab符 以及换行符
        c = s.toCharArray();

        if (c[c.length - 1] == ' ' | c[c.length - 1] == ';' | c[c.length - 1] == '\r' | c[c.length - 1] == '\n'
                | c[c.length - 1] != '\t') {
            int i;
            for (i = c.length - 1; i > -1; i--)
                if (c[i] != ' ' && c[i] != ';' && c[i] != '\r' && c[i] != '\n' && c[i] != '\t')
                    break;
            c = s.substring(0, i + 1).toCharArray();
            s = new String(c);
        }
        return s;

    }

    // 解析创建数据库语句
    public int create_database_interpret(String s) throws Exception {
        s = s.substring(16);// 因为create database *** 而*的位置刚好是第16个

        queryTime = new Date();
        millisecond = queryTime.getTime();

        if (HNUSQL.api.createDatabase(s) == 0) {
            queryTime = new Date();
            System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
        }
        return 0;
    }

    // 解析创建表的语句
    public int create_table_interpret(String s) throws Exception {
        String table_name;
        char[] c = s.toCharArray();
        String ss;

        int i;
        // 找到表名的位置
        for (i = 0; i < c.length; i++)
            if (c[i] == '(')
                break;

        table_name = clean(s.substring(12, i));// 获得表名
        // ss: 字段信息
        ss = clean(s.substring(i + 1, c.length - 1));

        if (ss.matches("([^\\s]+[\\s]*(char|int|float).*)+[\\s]*(primary[\\s]+key[\\s]*\\([\\s]*[^\\s]+[\\s]*\\))?")) {
            String[] attr_name;
            String[] attr_type;
            int[] type;
            boolean[] is_unique;
            String primary_key = "";
            boolean has_primary_key = false;

            ss = clean(ss);
            char[] cc = ss.toCharArray();

            if (ss.indexOf("primary key") != -1)// 如果存在primary key
            {
                for (i = ss.indexOf("primary key"); i < cc.length; i++) // primary key ( 列名)
                    if (cc[i] == '(')
                        break;
                primary_key = clean(ss.substring(i + 1, cc.length - 1)); // 获得列名

                ss = clean(ss.substring(0, ss.indexOf("primary key")));// 获取除primary key 之外的信息 也就是列名
                has_primary_key = true;
            }
            String[] fields = ss.split(","); // 将剩下的语句按照各个列名分隔开

            int j;
            attr_name = new String[fields.length];
            attr_type = new String[fields.length];
            is_unique = new boolean[fields.length];
            type = new int[fields.length];

            for (i = 0; i < fields.length; i++)// 获得列名和属性
            {
                fields[i] = clean(fields[i]);
                char[] temp = fields[i].toCharArray();
                for (j = 0; j < temp.length; j++)
                    if (temp[j] == ' ')
                        break;
                attr_name[i] = fields[i].substring(0, j);
                attr_type[i] = fields[i].substring(j + 1);
            }

            if (!has_primary_key)
                primary_key = attr_name[0];
            // 判断列名是否有重复的
            for (i = 0; i < attr_name.length - 1; i++) {
                for (j = i + 1; j < attr_name.length; j++)
                    if (attr_name[i].equals(attr_name[j])) {
                        System.out.println("*****Attributes must have different names!*****");
                        return 0;
                    }
            }

            boolean is_primary_true = false;
            for (i = 0; i < attr_type.length; i++) {
                if (attr_name[i].equals(primary_key))// 判断该列名是否是primary key
                    is_primary_true = true;

                is_unique[i] = false;
                if (attr_type[i].indexOf("unique") != -1)// 判断属性是否包含unique
                {
                    is_unique[i] = true;
                    attr_type[i] = clean(attr_type[i].substring(0, attr_type[i].indexOf("unique")));
                }
                // 判断属性的类型
                if (attr_type[i].equals("int"))
                    type[i] = -1;
                else if (attr_type[i].equals("float"))
                    type[i] = -2;
                else if (attr_type[i].indexOf("char") != -1) {
                    if (attr_type[i].matches("char[\\s]*\\([\\s]*\\d{1,3}[\\s]*\\)")) {
                        char[] ccc = attr_type[i].toCharArray();
                        // 接下来是获得char的长度
                        for (j = 0; j < ccc.length; j++)
                            if (ccc[j] == '(')
                                break;
                        attr_type[i] = clean(attr_type[i].substring(j + 1, ccc.length - 1));
                        type[i] = Integer.decode(attr_type[i]).intValue();
                        if (type[i] > 255) {
                            System.out.println("*****Type 'char' can't be longer than 255*****");
                            return 0;
                        }
                    } else {
                        System.out.println("*****Parameter Error*****");
                        return 0;
                    }
                }

            }
            // 如果根本就不存在primary key对应的列名
            if (!is_primary_true) {
                System.out.println("*****Primary Key Wrong!*****");
                return 0;
            }

            queryTime = new Date();
            millisecond = queryTime.getTime();
            // 解析完毕 创建表
            if (HNUSQL.api.createTable(table_name, attr_name, type, is_unique, primary_key) == 0) {
                queryTime = new Date();
                System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
            }
            return 0;
        } else {
            System.out.println("*****Paramater Error*****");
            return 0;
        }

    }

    // 解析创建索引语句
    public int create_index_interpret(String s) {
        String index_name;
        String table_name;
        String attr_name;
        char[] c = s.toCharArray();
        int i, j, k;
        for (i = 13; i < c.length; i++)
            if (c[i] == ' ')
                break;
        index_name = s.substring(13, i);// 获取索引名
        for (j = i + 4; j < c.length; j++)
            if (c[j] == '(')
                break;
        table_name = s.substring(i + 4, j);// 获取表名
        for (k = j; k < c.length; k++) // 获取列名的第一个字符
            if (c[k] != ' ' && c[k] != '(')
                break;
        for (i = k + 1; i < c.length; i++)// 获取列名最后的括号
            if (c[i] == ' ' | c[i] == ')')
                break;
        attr_name = s.substring(k, i);// 获取列名

        queryTime = new Date();
        millisecond = queryTime.getTime();
        // 开始创建索引
        if (HNUSQL.api.createIndex(index_name, table_name, attr_name) == 0) {
            queryTime = new Date();
            System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
        }
        return 0;
    }

    // 解析选择数据库语句
    public int use_interpret(String s) throws Exception {
        s = clean(s.substring(3)); // 获取数据库名

        queryTime = new Date();
        millisecond = queryTime.getTime();

        if (HNUSQL.api.useDatabase(s) == 0) {
            queryTime = new Date();
            System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
        }
        return 0;
    }

    // 解析删除数据库语句
    public int drop_database_interpret(String s) throws Exception {
        s = clean(s.substring(13)); // 获取要删除的数据库名

        queryTime = new Date();
        millisecond = queryTime.getTime();

        if (HNUSQL.api.dropDatabase(s) == 0) {
            queryTime = new Date();
            System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
        }
        return 0;
    }

    // 解析删除表的语句
    public int drop_table_interpret(String s) {
        s = clean(s.substring(10));// 获取表名

        queryTime = new Date();
        millisecond = queryTime.getTime();

        if (HNUSQL.api.dropTable(s) == 0) {
            queryTime = new Date();
            System.out.println("\nQuery Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
        }
        return 0;
    }

    // 解析删除索引语句
    public int drop_index_interpret(String s) {
        s = clean(s.substring(10)); // 获取索引名

        queryTime = new Date();
        millisecond = queryTime.getTime();

        if (HNUSQL.api.dropIndex(s) == 0) {
            queryTime = new Date();
            System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
        }
        return 0;
    }

    // 解析插入语句
    public int insert_interpret(String s) throws Exception {
        String table_name;
        char[] c = s.toCharArray();
        int i;

        for (i = 12; i < c.length; i++)
            if (c[i] == ' ')
                break;
        table_name = clean(s.substring(12, i));// 得到表名

        for (i = 0; i < c.length; i++)
            if (c[i] == '(')
                break;
        s = clean(s.substring(i + 1, c.length - 1));

        if (s.matches("(.+,[\\s]*)*[\\s]*[^\\s]+")) {
            String[] fields;
            String[] attribute;
            int[] attr_type;

            int j, k;
            fields = s.split(",");
            attribute = new String[fields.length];
            attr_type = new int[fields.length];

            for (i = 0; i < fields.length; i++) {
                if (fields[i].matches("([\\s]*\".*\"[\\s]*)|([\\s]*\'.*\'[\\s]*)"))// 匹配字符
                {
                    c = fields[i].toCharArray();
                    for (j = 0; j < c.length; j++)
                        if (c[j] == '\"' | c[j] == '\'')
                            break;
                    for (k = j + 1; k < c.length; k++)
                        if (c[k] == '\"' | c[k] == '\'')
                            break;
                    attribute[i] = fields[i].substring(j + 1, k);// 获取“**”或者‘***’的数据
                    attr_type[i] = attribute[i].length();
                } else {
                    if (fields[i].matches("[\\s]*-?[\\d]+[\\s]*"))// 匹配int
                    {
                        attribute[i] = clean(fields[i]);
                        attr_type[i] = -1;
                    } else {
                        if (fields[i].matches("[\\s]*\\d+\\.\\d+[\\s]*"))// 匹配float
                        {
                            attribute[i] = clean(fields[i]);
                            attr_type[i] = -2;
                        } else {
                            System.out.println("*****Parameter Error*****");
                            return 0;
                        }
                    }
                }
            }

            queryTime = new Date();
            millisecond = queryTime.getTime();
            /**
             * 将记录插入
             */
            if (HNUSQL.api.insertRecord(table_name, attribute, attr_type) == 0) {
                queryTime = new Date();
                System.out
                        .println("\nQuery Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
            }
            return 0;
        } else {
            System.out.println("*****Parameter Error*****");
            return 0;
        }

    }

    // 解析删除语句
    public int delete_interpret(String s) throws Exception {
        String table_name;
        String[] condition;

        char[] c = s.toCharArray();
        int i, j, k;

        for (i = 12; i < c.length; i++)
            if (c[i] == ' ')
                break;
        table_name = clean(s.substring(11, i));// 获取表名

        if (s.matches("delete from [^\\s]+"))// 非条件删除
        {
            HNUSQL.api.deleteRecord(table_name);
            return 0;
        } else if (s.matches("delete from [^\\s]+ where .*( and .*)*"))// 条件删除
        {
            for (i = i + 1; i < c.length; i++)
                if (c[i] == ' ')
                    break;
            s = clean(s.substring(i));// 获取where后面的语句

            condition = s.split("and");

            String[] con_attr_name = new String[condition.length];
            String[] con_attr = new String[condition.length];
            int[] con_attr_type = new int[condition.length];
            int[] operation = new int[condition.length];

            for (i = 0; i < condition.length; i++) {
                condition[i] = clean(condition[i]);
                if (!condition_check(condition[i])) {
                    System.out.println("******Condition Error*****");
                    return 0;
                }
                c = condition[i].toCharArray();
                for (j = 0; j < c.length; j++)
                    if (c[j] == '=' | c[j] == '>' | c[j] == '<')
                        break;
                con_attr_name[i] = clean(condition[i].substring(0, j));
                for (k = j + 1; k < c.length; k++)
                    if (c[k] != '=' && c[k] != '>' && c[k] != '<')
                        break;
                operation[i] = operation_check(clean(condition[i].substring(j, k)));
                con_attr[i] = clean(condition[i].substring(k));
                if (type_check(con_attr[i]) < -2) {
                    System.out.println("*****Variable Type Error*****");
                    return 0;
                } else if (type_check(con_attr[i]) >= 0) {
                    con_attr[i] = con_attr[i].substring(1, con_attr[i].length() - 1);
                }

                con_attr_type[i] = type_check(con_attr[i]);
            }

            queryTime = new Date();
            millisecond = queryTime.getTime();

            if (HNUSQL.api.deleteRecord(table_name, con_attr_name, con_attr, con_attr_type, operation) == 0) {
                queryTime = new Date();
                System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
            }
            return 0;
        }
        // 条件删除结束
        else {
            System.out.println("*****Syntax Error*****");
            return 0;
        }
    }

    /**
     * 更新功能粗略实现
     *
     * @param s
     * @return
     */
    public int update_interpret(String s) throws Exception {

        char[] c = s.toCharArray();
        int i, j;
        int count = 0;
        String[] attr = new String[20];
        j = 7;
        for (i = 7; i < c.length; i++) {
            if (c[i] == ' ') {
                attr[count++] = clean(s.substring(j, i));
                j = i;
            }
        }
        attr[count] = clean(s.substring(j, c.length));
        String table_name = attr[0];
        String target_name = attr[2].split("=")[0];
        String target_value = attr[2].split("=")[1];
        int target_type = -1;

        String[] con_attr_name = new String[1];
        String[] con_attr = new String[1];
        int[] con_attr_type = new int[1];
        int[] operation = new int[1];

        int index = -1;
        int operation_code = 1;
        int num = 0;
        if (attr[4].indexOf("=") != -1) {
            index = attr[4].indexOf("=");
            num = 1;
            operation_code = 1;
        } else if (attr[4].indexOf("<>") != -1) {
            index = attr[4].indexOf("<>");
            num = 2;
            operation_code = 2;
        } else if (attr[4].indexOf("<") != -1) {
            index = attr[4].indexOf("<");
            num = 1;
            operation_code = 3;
        } else if (attr[4].indexOf(">") != -1) {
            index = attr[4].indexOf(">");
            num = 1;
            operation_code = 4;
        } else if (attr[4].indexOf("<=") != -1) {
            index = attr[4].indexOf("<=");
            num = 2;
            operation_code = 5;
        } else if (attr[4].indexOf(">=") != -1) {
            index = attr[4].indexOf(">=");
            num = 2;
            operation_code = 6;
        }

        con_attr_name[0] = attr[4].substring(0, index);
        String str = attr[4].substring(index + num);
        con_attr[0] = str.replaceAll("'", "");
        operation[0] = operation_code;
        con_attr_type[0] = type_check(str);

        queryTime = new Date();
        millisecond = queryTime.getTime();
        if (HNUSQL.api.updateRecord(table_name, target_name, target_value, target_type, con_attr_name, con_attr,
                con_attr_type, operation) == 0) {
            queryTime = new Date();
            System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
        }
        return 0;

    }

    // 解析选择记录语句
    public int select_interpret(String s) throws Exception {
        String table_name;
        int i, j, k;

        if (s.matches("select \\* from [^\\s]+")) {
            table_name = clean(s.substring(13));// 获得表名

            queryTime = new Date();
            millisecond = queryTime.getTime();

            if (HNUSQL.api.selectRecord(table_name) == 0) {
                queryTime = new Date();
                System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
            }
            return 0;
        } else if (s.matches("select \\* from [^\\s]+ where .*( and .*)*")) {
            String[] condition;

            char[] c = s.toCharArray();
            for (i = 14; i < c.length; i++)
                if (c[i] == ' ')
                    break;
            table_name = clean(s.substring(13, i));
            s = clean(s.substring(i + 6));

            condition = s.split("and");

            String[] con_attr_name = new String[condition.length];
            String[] con_attr = new String[condition.length];
            int[] con_attr_type = new int[condition.length];
            int[] operation = new int[condition.length];

            for (i = 0; i < condition.length; i++) {
                condition[i] = clean(condition[i]);
                if (!condition_check(condition[i]))// 条件有问题
                {
                    System.out.println("******Condition Error*****");
                    return 0;
                }
                c = condition[i].toCharArray();
                for (j = 0; j < c.length; j++)
                    if (c[j] == '=' | c[j] == '>' | c[j] == '<')
                        break;
                con_attr_name[i] = clean(condition[i].substring(0, j)); // 条件属性名
                for (k = j + 1; k < c.length; k++)
                    if (c[k] != '=' && c[k] != '>' && c[k] != '<')
                        break;
                operation[i] = operation_check(clean(condition[i].substring(j, k)));
                // con_attr_type[i] = type_check(con_attr[i]);
                con_attr[i] = clean(condition[i].substring(k));// 获取条件属性值

                if (type_check(con_attr[i]) < -2)// 变量类型不是char int float三个之一
                {
                    System.out.println("*****Variable Type Error*****");
                    return 0;
                }
                // 对属性类型赋值
                else if (type_check(con_attr[i]) == -1 | type_check(con_attr[i]) == -2) {
                    con_attr_type[i] = type_check(con_attr[i]);
                } else if (type_check(con_attr[i]) >= 0) {
                    con_attr[i] = con_attr[i].substring(1, con_attr[i].length() - 1);
                    con_attr_type[i] = type_check("\"" + con_attr[i] + "\"");
                }

            }

            queryTime = new Date();
            millisecond = queryTime.getTime();

            if (HNUSQL.api.selectRecord(table_name, con_attr_name, con_attr, con_attr_type, operation) == 0) {
                queryTime = new Date();
                System.out.println("Query Time : " + (float) (queryTime.getTime() - millisecond) / 1000 + " seconds!");
            }
            return 0;
        } else {
            System.out.println("*****Sorry, You should use select * from……*****");
            return 0;
        }
    }

    /**
     * 解析执行文件
     *
     * @param s
     * @return
     * @throws Exception
     */
    public int execfile_interpret(String s) throws Exception {
        s = clean(s.substring(8));// 获得文件名
        File ff = new File(s);
        if (!ff.exists()) {
            ff = new File(HNUSQL.MINISQL_PATH + File.separator + s);
            if (!ff.exists()) {
                System.out.println("*****File " + ff.getAbsolutePath() + " not exist!*****");
                return 0;
            }
        }

        BufferedReader br = new BufferedReader(new FileReader(ff));// 读取文件
        char[] c = new char[(int) (ff.length())];// 将文件中所有的数据转存为char数组
        char[] temp;
        String ss;
        br.read(c, 0, c.length);
        int i, j;
        i = 0;
        for (j = 0; j < c.length; j++) {
            if (c[j] == ';') {
                temp = new char[j - i + 1];
                for (int k = 0; k < temp.length; k++)
                    temp[k] = c[i + k];
                ss = new String(temp);
                interpret(ss.toLowerCase());
                i = j + 1;
            }
        }
        br.close();
        return 0;
    }

    /**
     * 检查s是否属于int char float 之一
     *
     * @param s
     * @return
     */
    public boolean condition_check(String s) {
        if (s.matches("[^\\s]+[\\s]*(=|<>|<|>|<=|>=)[\\s]*[^\\s].*")) {
            char[] c = s.toCharArray();
            int i;
            for (i = 0; i < c.length; i++)
                if (c[i] == '=' | c[i] == '>' | c[i] == '<')
                    break;
            for (i = i + 1; i < c.length; i++)
                if (c[i] != '=' && c[i] != '>' && c[i] != '<')
                    break;
            if (type_check(clean(s.substring(i))) < -2)// 不是int char float之一
                return false;
            else
                return true;
        } else
            return false;
    }

    /**
     * 判断字符是什么类型
     *
     * @param s
     * @return
     */
    public int type_check(String s) {
        // 字符串类型
        if (s.matches("([\\s]*\".*\"[\\s]*)|([\\s]*\'.*\'[\\s]*)")) {
            return clean(s).length() - 2;
        }
        // 整型
        else if (s.matches("^-?[1-9]\\d*$")) {
            return -1;
        }
        // 浮点型
        else if (s.matches("^[1-9]\\d*\\.\\d*|0\\.\\d*[1-9]\\d*$")
                || s.matches("^-[1-9]\\d*\\.\\d*|-0\\.\\d*[1-9]\\d*$")) {
            return -2;
        }
        // 未知类型
        else {
            return -10;
        }
    } // 将操作符转为int

    public int operation_check(String s) {
        if (s.equals("="))
            return 1;
        if (s.equals("<>"))
            return 2;
        if (s.equals("<"))
            return 3;
        if (s.equals(">"))
            return 4;
        if (s.equals("<="))
            return 5;
        if (s.equals(">="))
            return 6;
        return 0;
    }

    // 显示已经存在的数据库
    public void show_database_interpret() {
        File ff = new File(HNUSQL.MINISQL_PATH + File.separator + "data");
        File[] filelist = ff.listFiles();
        for (int i = 0; i < filelist.length; i++) {
            if (filelist[i].isDirectory()) {
                System.out.println(filelist[i].getName());
            }
        }
    }

    // 显示存在的表
    public void show_table_interpret() {
        HNUSQL.api.show_table();
    }
}
