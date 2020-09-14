package com.mytoy.starter.tools;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;

/**
 * 扩展JSON
 */
public class MyJSON {
    public static String toJSONString(Object object) {
        if (null == object) return "{}";
        if (object instanceof String && StringUtils.equals("", (String) object)) return "{}";
        return JSON.toJSONString(object);
    }
}
