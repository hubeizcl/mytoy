package com.mytoy.starter.tools;

import java.util.*;

public class MyMap<K, V> {

    private Map<K, V> map;

    private List<K> keys = new ArrayList<>();

    private List<V> values = new ArrayList<>();

    private MyMap() {
        this.map = new HashMap();
    }

    public static <K, V> MyMap<K, V> builder() {
        return new MyMap<>();
    }

    public MyMap<K, V> of(K k, V v) {
        this.map.put(k, v);
        return this;
    }

    public MyMap<K, V> ofs(Map<K, V> map) {
        if (isNotEmpty(map)) this.map.putAll(map);
        return this;
    }

    public MyMap<K, V> keys(K... ks) {
        if (MyArrays.isNotEmpty(ks)) this.keys.addAll(Arrays.asList(ks));
        return this;
    }


    public MyMap<K, V> values(V... vs) {
        if (MyArrays.isNotEmpty(vs)) this.values.addAll(Arrays.asList(vs));
        return this;
    }

    public Map<K, V> build() {
        if (null != keys && keys.size() > 0 && null != values && values.size() > 0 && keys.size() == values.size())
            for (int i = 0; i < keys.size(); i++) this.map.put(keys.get(i), values.get(i));
        return this.map;
    }

    public static <K, V> boolean isNotEmpty(Map<K, V> map) {
        if (null != map && map.size() > 0) return true;
        return false;
    }

    public static <K, V> boolean isEmpty(Map<K, V> map) {
        return !isNotEmpty(map);
    }

    /**
     * 交集
     */
    public static <K, V> Map<K, V> intersection(Map<K, V> src, Map<K, V> dst) {
        if (isEmpty(src) && isEmpty(dst)) return new HashMap<>();
        if (isEmpty(src)) return new HashMap<>();
        if (isEmpty(dst)) return new HashMap<>();
        MyMap<K, V> builder = MyMap.builder();
        Iterator<K> iterator = src.keySet().iterator();
        Set<K> ks = dst.keySet();
        while (iterator.hasNext()) {
            K next = iterator.next();
            if (ks.contains(next)) builder.of(next, src.get(next));
        }
        return builder.build();
    }

    /**
     * 并集
     */
    public static <K, V> Map<K, V> union(Map<K, V> map1, Map<K, V> map2) {
        if (isEmpty(map1)) map1 = new HashMap<>();
        if (isEmpty(map2)) map2 = new HashMap<>();
        if (isEmpty(map1) && isEmpty(map2)) return map1;
        if (null == map1) return map2;
        if (null == map2) return map1;
        map1.putAll(map2);
        return map1;
    }

    /**
     * 差集
     */
    public static <K, V> Map<K, V> complement(Map<K, V> src, Map<K, V> dst) {
        if (isEmpty(src) && isEmpty(dst)) return new HashMap<>();
        if (isEmpty(src)) return dst;
        if (isEmpty(dst)) return src;
        Map<K, V> intersection = intersection(src, dst);
        Map<K, V> union = union(src, dst);
        MyMap<K, V> builder = MyMap.builder();
        Iterator<K> iterator = union.keySet().iterator();
        Set<K> ks = intersection.keySet();
        if (iterator.hasNext()) {
            K next = iterator.next();
            if (!ks.contains(next)) builder.of(next, union.get(next));
        }
        return builder.build();
    }
}
