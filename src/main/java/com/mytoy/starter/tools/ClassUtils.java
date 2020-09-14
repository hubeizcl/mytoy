package com.mytoy.starter.tools;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class ClassUtils {

    /**
     *获取泛型的class类型
     */
    public static <T> Class<T> getGenericSuperclass(T t) {
        Type genType = t.getClass().getGenericSuperclass();
        Type[] params = ((ParameterizedType) genType).getActualTypeArguments();
        return (Class) params[0];
    }

}
