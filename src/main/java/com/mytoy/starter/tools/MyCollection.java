package com.mytoy.starter.tools;

import java.util.*;

public class MyCollection<T> {

    private Collection<T> collection;

    private MyCollection() {
        collection = new ArrayList<>();
    }

    public static <T> MyCollection<T> builder() {
        return new MyCollection<>();
    }

    public MyCollection<T> of(T... ts) {
        if (null != ts && ts.length > 0) for (T t : ts) collection.add(t);
        return this;
    }

    public MyCollection<T> of(Collection<T> ts) {
        if (null != ts && ts.size() > 0) for (T t : ts) of(t);
        return this;
    }

    public Collection<T> build() {
        return this.collection;
    }

    public List<T> build2List() {
        return new ArrayList<>(this.collection);
    }

    public Set<T> build2Set() {
        return new HashSet<>(this.collection);
    }

    public T[] toArray(Class<T> tClass) {
        return MyArrays.toArray(collection, tClass);
    }

    public static <T> T[] toArray(Collection<T> collection, Class<T> tClass) {
        return MyArrays.toArray(collection, tClass);
    }

    public static <T> List<T> toList(T... ts) {
        List<T> tList = MyCollection.<T>builder().of(ts).build2List();
        return tList;
    }

    public static <T> Set<T> toSet(T... ts) {
        Set<T> tSet = MyCollection.<T>builder().of(ts).build2Set();
        return tSet;
    }

    public static <T> boolean isNotEmpty(Collection<T> collection) {
        if (null != collection && collection.size() > 0) return true;
        return false;
    }

    public static <T> boolean isEmpty(Collection<T> collection) {
        return !isNotEmpty(collection);
    }

    public static <T> Collection<T> baseAddAll(Collection<T> list, T... ts) {
        Collection<T> connection;
        if (null == ts) connection = MyCollection.<T>builder().build();
        else connection = MyCollection.<T>builder().of(ts).build();
        return baseMerge(list, connection);
    }

    public static <T> Collection<T> baseMerge(Collection<T> src, Collection<T> dst) {
        if (isNotEmpty(src) && isNotEmpty(dst)) {
            Iterator<T> iterator = src.iterator();
            while (iterator.hasNext()) dst.add(iterator.next());
            return dst;
        } else if (isNotEmpty(src)) return src;
        else if (isNotEmpty(dst)) return dst;
        return MyCollection.<T>builder().build();
    }

    public static <T> List<T> merge(List<T> list, T... ts) {
        Collection<T> collection = baseAddAll(list, ts);
        return new ArrayList<>(collection);
    }

    public static <T> Set<T> merge(Set<T> set, T... ts) {
        Collection<T> collection = baseAddAll(set, ts);
        return new HashSet<>(collection);
    }

    public static <T> List<T> merge(List<T> src, List<T> dst) {
        Collection<T> collection = baseMerge(src, dst);
        return new ArrayList<>(collection);
    }

    public static <T> List<T> merge(List<T> src, Set<T> dst) {
        Collection<T> collection = baseMerge(src, dst);
        return new ArrayList<>(collection);
    }

    public static <T> Set<T> merge(Set<T> src, Set<T> dst) {
        Collection<T> collection = baseMerge(src, dst);
        return new HashSet<>(collection);
    }

    public static <T> Set<T> merge(Set<T> src, List<T> dst) {
        Collection<T> collection = baseMerge(src, dst);
        return new HashSet<>(collection);
    }

    /**
     * 交集
     */
    public static <T> Collection<T> intersection(Collection<T> src, Collection<T> dst) {
        if (isEmpty(src) && isEmpty(dst)) return new HashSet<>();
        if (isEmpty(src)) return new HashSet<>();
        if (isEmpty(dst)) return new HashSet<>();
        MyCollection<T> builder = MyCollection.builder();
        if (isNotEmpty(src) && isNotEmpty(dst)) {
            Iterator<T> iterator = src.iterator();
            while (iterator.hasNext()) {
                T next = iterator.next();
                if (dst.contains(next)) builder.of(next);
            }
        }
        return new HashSet<>(builder.build());
    }

    /**
     * 并集
     */
    public static <T> Collection<T> union(Collection<T> src, Collection<T> dst) {
        if (isEmpty(src) && isEmpty(dst)) return new HashSet<>();
        if (isEmpty(src)) return new HashSet(dst);
        if (isEmpty(dst)) return new HashSet(src);
        Collection<T> collection = baseMerge(src, dst);
        if (isNotEmpty(collection)) return new HashSet(collection);
        return collection;
    }

    /**
     * 差集
     */
    public static <T> Collection complement(Collection<T> src, Collection<T> dst) {
        if (isEmpty(src) && isEmpty(dst)) return new HashSet<>();
        if (isEmpty(src)) return new HashSet(dst);
        if (isEmpty(dst)) return new HashSet(src);
        Collection<T> intersection = intersection(src, dst);
        Collection<T> union = union(src, dst);
        MyCollection<T> builder = MyCollection.builder();
        Iterator<T> iterator = union.iterator();
        if (iterator.hasNext()) {
            T next = iterator.next();
            if (!intersection.contains(next)) builder.of(next);
        }
        return builder.build();
    }
}
