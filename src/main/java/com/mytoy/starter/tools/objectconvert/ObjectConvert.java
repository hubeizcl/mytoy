package com.mytoy.starter.tools.objectconvert;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ObjectConvert {

    private static final Logger log = LoggerFactory.getLogger(ObjectConvert.class);

    public static <T, E> T convert(E e, Class<T> type) {
        if (null != e)
            return JSON.parseObject(JSON.toJSONString(e), type);
        return null;
    }

    public static <T, E> List<T> converts(List<E> eList, Class<T> type) {
        List<T> ts = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(eList)) {
            eList.forEach(e -> ts.add(convert(e, type)));
        }
        return ts;
    }

}
