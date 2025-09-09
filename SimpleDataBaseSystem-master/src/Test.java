import javax.xml.bind.Binder;
import java.lang.String;
import java.io.*;
import java.util.*;

/**
 * 说明: <br>
 *
 * @author ZSCDumin <br>
 *         邮箱: 2712220318@qq.com <br>
 *         日期: 2018/11/14 <br>
 *         版本: 1.0
 */
public class Test {
    public static void main(String[] args) {
        String str = "addd";
        System.out.println(type_check(str));
    }

    public static int type_check(String s) {
        if (s.matches("([\\s]*\".*\"[\\s]*)|([\\s]*\'.*\'[\\s]*)"))// char
            return Interpreter.clean(s).length() - 2;
        else if (s.matches("^-?[1-9]\\d*$"))// int
            return -1;
        else if (s.matches("^[1-9]\\d*\\.\\d*|0\\.\\d*[1-9]\\d*$")
                || s.matches("^-[1-9]\\d*\\.\\d*|-0\\.\\d*[1-9]\\d*$"))
            return -2;
        else
            return -10;
    }
}
