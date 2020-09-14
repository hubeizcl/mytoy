package com.mytoy.starter.tools;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class MyString {
    /**
     * 下划线转驼峰
     */
    public static String underline2Camel(String str) {
        if (StringUtils.isNotBlank(str)) {
            char[] chars = str.toCharArray();
            boolean flag = false;
            char[] newChars = new char[chars.length];
            int index = 0;
            for (char aChar : chars) {
                if (aChar == ' ') continue;
                if (aChar == '_') {
                    flag = true;
                    continue;
                }
                if (flag) {
                    if (aChar > 'a' && aChar < 'z') aChar -= 32;
                    flag = false;
                }
                newChars[index] = aChar;
                index++;
            }
            str = String.valueOf(newChars).substring(0, index);
        }
        return str;
    }

    public static String underline2CamelAndFirstUpper(String str) {
        str = underline2Camel(str);
        if (null != str && !"".equals(str.trim())) {
            char[] chars = str.toCharArray();
            for (int i = 0; i < chars.length; i++) {
                if (i == 0) {
                    if (chars[0] >= 'a' && chars[0] <= 'z') {
                        chars[0] -= 32;
                    }
                    break;
                }
            }
            str = new String(chars);
        }
        return str;
    }

    /**
     * 驼峰转下划线
     */
    public static String camel2Underline(String str) {
        if (StringUtils.isNotBlank(str)) {
            char[] chars = str.toCharArray();
            char[] newChars = new char[2 * chars.length];
            int index = 0;
            for (char aChar : chars) {
                if (aChar >= 'A' && aChar <= 'Z') {
                    aChar += 32;
                    newChars[index] = '_';
                    index++;
                }
                newChars[index] = aChar;
                index++;
            }
            str = String.valueOf(newChars).substring(0, index);
        }
        return str;
    }

    public static boolean equals(String str1, String str2) {
        if (null != str1 && null != str2 && str1.equals(str2)) return true;
        return false;
    }

    public static boolean isNotBlank(String string) {
        if (null != string && !"".equals(string.trim())) return true;
        return false;
    }

    public static boolean isBlank(String string) {
        return !isNotBlank(string);
    }

    public static String delComma(String str) {
        if (isNotBlank(str)) {
            str = str.replaceAll(",,", ",");
            if (str.contains(",,")) delComma(str);
        }
        return str;
    }

    public static String choice(String str1, String str2) {
        if (isNotBlank(str1)) return str1;
        return str2;
    }

    public static Boolean contains(String str1, String str2) {
        if (isNotBlank(str1)) str1.contains(str2);
        return false;
    }

    public static String[] split4Line(String str) {
        if (isNotBlank(str))
            return MyArrays.delBlank(str.split("\\r?\\n", -1));
        return new String[]{};
    }

    public static String next(List<String> list, String str) {
        if (MyCollection.isNotEmpty(list) && isNotBlank(str) && list.contains(str)) {
            int i = list.indexOf(str);
            if (i < list.size() - 1) {
                return list.get(i + 1);
            }
        }
        return "";
    }

    public static List<Integer> getIndex4Pattern(String str, String pattern) {
        List<Integer> integers = new ArrayList<>();
        if (isNotBlank(str)) {
            String[] split = str.split(pattern);
            int i = 0;
            for (String s : split) {
                if (!"".equals(s)) {
                    i += s.length();
                }
                integers.add(i);
                i += 1;
            }
            Integer integer = integers.get(integers.size() - 1);
            if (integer < str.length()) {
                for (int j = integer; j < str.length(); j++) {
                    integers.add(j);
                }
            }
        }
        return integers.subList(0, integers.size() - 1);
    }


    public static List<String> splitAndDeleteBlank(String str, String s) {
        List<String> list = new ArrayList<>();
        if (isNotBlank(str)) {
            String[] split = str.split(s);
            if (MyArrays.isNotEmpty(split)) for (String s1 : split) if (isNotBlank(s1)) list.add(s1);
        }
        return list;
    }
}
