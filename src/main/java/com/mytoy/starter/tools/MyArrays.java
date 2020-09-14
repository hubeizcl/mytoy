package com.mytoy.starter.tools;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Function;


public class MyArrays<T> {

    public static final String[] EMPTY_STRING_ARRAY = getEmptyArray(String.class);

    private Collection<T> collection;

    private Class<T> clazz;

    private MyArrays() {
        collection = new ArrayList<>();
    }

    public static <T> MyArrays<T> builder() {
        return new MyArrays<>();
    }

    public MyArrays<T> of(T... ts) {
        if (null != ts && ts.length > 0) for (T t : ts) {
            if (null == clazz) clazz = ClassUtils.getGenericSuperclass(t);
            collection.add(t);
        }
        return this;
    }

    public MyArrays<T> clazz(Class<T> clazz) {
        if (null == this.clazz)
            this.clazz = clazz;
        return this;
    }

    public MyArrays<T> of(Collection<T> ts) {
        if (null != ts && ts.size() > 0) for (T t : ts) of(t);
        return this;
    }

    public T[] build() {
        return toArray(collection, clazz);
    }

    public List<T> build2List() {
        return new ArrayList<>(this.collection);
    }

    public Set<T> build2Set() {
        return new HashSet<>(this.collection);
    }

    public static <T> T[] createFixedLengthArray(Class<T> clazz, int length) {
        return (T[]) Array.newInstance(clazz, length);
    }

    public static <T> T[] getEmptyArray(Class<T> clazz) {
        return createFixedLengthArray(clazz, 0);
    }


    public static <T> boolean isNotEmpty(T... ts) {
        if (null != ts && ts.length > 0) return true;
        return false;
    }

    public static <T> boolean isEmpty(T... ts) {
        return !isNotEmpty(ts);
    }

    public static <T> T[] toArray(Collection<T> collection, Class<T> clazz) {
        if (MyCollection.isEmpty(collection)) return createFixedLengthArray(clazz, 0);
        return toArray(clazz).apply(collection);
    }

    public static String[] toArray(Collection<String> collection) {
        return toArray(collection, String.class);
    }

    public static <T> T[] merge(T[] src, T[] dst, Class<T> clazz) {
        return merge(clazz).apply(src).apply(dst);
    }

    public static String[] merge(String[] src, String[] dst) {
        return merge(String.class).apply(src).apply(dst);
    }

    public static String[] merge(String[] src, String dst) {
        return merge(String.class).apply(src).apply(new String[]{dst});
    }

    public static String[] delBlank(String[] src) {
        List<String> list = new ArrayList<>();
        if (isNotEmpty(src)) {
            for (String s : src) {
                if (!MyString.equals("", s)) {
                    list.add(s);
                }
            }
        }
        return toArray(list);
    }

    public static <T> T[] toArray(Collection<T> src, Collection<T> dst, Class<T> clazz) {
        if (MyCollection.isEmpty(src) && MyCollection.isEmpty(dst)) return createFixedLengthArray(clazz, 0);
        return mergeCollect(clazz).apply(src).apply(dst);
    }

    public static <T> Function<Collection<T>, T[]> toArray(Class<T> clazz) {
        return vo -> {
            if (isNotEmpty(vo)) {
                T[] ts = createFixedLengthArray(clazz, vo.size());
                Iterator<T> iterator = vo.iterator();
                int i = 0;
                while (iterator.hasNext()) {
                    ts[i] = iterator.next();
                    i++;
                }
                return ts;
            }
            return createFixedLengthArray(clazz, 0);
        };

    }

    public static <T> Function<T[], Function<T[], T[]>> merge(Class<T> clazz) {
        return src -> dst -> {
            if (null == src || src.length == 0) return dst;
            if (null == dst || dst.length == 0) return src;
            T[] result = createFixedLengthArray(clazz, src.length + dst.length);
            for (int i = 0; i < src.length; i++) result[i] = src[i];
            for (int i = 0; i < dst.length; i++) result[i + src.length] = dst[i];
            return result;
        };
    }

    public static <T> Function<Collection<T>, Function<Collection<T>, T[]>> mergeCollect(Class<T> clazz) {
        return src -> dst -> {
            T[] ts1 = createFixedLengthArray(clazz, 0);
            if (null != src && src.size() > 0) ts1 = toArray(src, clazz);
            T[] ts2 = createFixedLengthArray(clazz, 0);
            if (null != dst && dst.size() > 0) ts2 = toArray(src, clazz);
            return merge(clazz).apply(ts1).apply(ts2);
        };
    }
}
