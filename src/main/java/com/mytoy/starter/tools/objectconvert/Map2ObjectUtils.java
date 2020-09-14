package com.mytoy.starter.tools.objectconvert;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author zhangchenglong8
 */
public class Map2ObjectUtils {

    private final static Logger logger = LoggerFactory.getLogger(Map2ObjectUtils.class);

    public static <T> T deepClone(Map<String, Object> srcMap, Class<T> clazz) {
        try {
            return deepClone(srcMap, clazz.newInstance());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }


    public static <T> T deepClone(Map<String, Object> srcMap, T dstObj) {
        return deepClone(srcMap, dstObj, ArrayUtils.EMPTY_STRING_ARRAY, ArrayUtils.EMPTY_STRING_ARRAY);
    }


    public static <T> T deepClone(Map<String, Object> srcMap, T dstObj, String[] includeField, String[] excludeField) {
        if (MapUtils.isEmpty(srcMap) || null == dstObj) return dstObj;
        Class dstObjClass = dstObj.getClass();
        Map<String, Field> realPropertyName = getRealPropertyName(dstObjClass);
        List<String> includeFieldList = ArrayUtils.isNotEmpty(includeField) ? Arrays.asList(includeField) : Lists.newArrayList();
        List<String> excludeFieldList = ArrayUtils.isNotEmpty(excludeField) ? Arrays.asList(excludeField) : Lists.newArrayList();
        if (MapUtils.isNotEmpty(srcMap)) {
            for (Map.Entry<String, Object> entry : srcMap.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();
                if (excludeFieldList.contains(fieldName)) continue;
                if (includeFieldList.contains(fieldName) || includeFieldList.isEmpty()) {
                    try {
                        Field dstObjClassField = realPropertyName.get(fieldName);
                        if (Objects.isNull(dstObjClassField)) continue;
                        if (Modifier.isFinal(dstObjClassField.getModifiers()))
                            continue;//如果目标对象属性是final,因为是属于类的属性，不能修改，跳过
                        String srcMapValueTypeName = value.getClass().getTypeName();
                        String dstObjClassFieldTypeName = dstObjClassField.getType().getTypeName();
                        dstObjClassField.setAccessible(true);
                        if (StringUtils.equals(srcMapValueTypeName, dstObjClassFieldTypeName)) {
                            dstObjClassField.set(dstObj, value);
                            continue;
                        }
                        if (ObjectCopyUtils.numType4List().contains(srcMapValueTypeName) && ObjectCopyUtils.numType4List().contains(dstObjClassFieldTypeName)) {
                            ObjectCopyUtils.packaging(dstObj, dstObjClassField, srcMapValueTypeName, dstObjClassFieldTypeName, value);//数字类型自动转换
                        } else {
                            if ((StringUtils.equals(ObjectCopyUtils.getBasicType4String(), srcMapValueTypeName) && ObjectCopyUtils.numType4List().contains(dstObjClassFieldTypeName))
                                    || (StringUtils.equals(ObjectCopyUtils.getBasicType4String(), dstObjClassFieldTypeName) && ObjectCopyUtils.numType4List().contains(srcMapValueTypeName))) {
                                ObjectCopyUtils.StringConversion(dstObj, dstObjClassField, srcMapValueTypeName, dstObjClassFieldTypeName, value);//基本类型，包装类，BigDecimal和String之间的自动转换，當然能轉換的前提是，能夠將String解析成相應的值
                            } else {
                                if (value instanceof Map)
                                    dstObjClassField.set(dstObj, deepClone((Map) value, dstObjClassField.getType().newInstance(), null, null));//递归调用
                            }
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            }
        }
        return dstObj;
    }


    private static Map<String, Field> getRealPropertyName(Class dstObjClass) {
        Map<String, Field> map = Maps.newHashMap();
        if (!Objects.isNull(dstObjClass)) {
            Field[] declaredFields = dstObjClass.getDeclaredFields();
            if (ArrayUtils.isNotEmpty(declaredFields)) {
                for (Field declaredField : declaredFields) {
                    declaredField.setAccessible(true);
                    JsonProperty annotation = declaredField.getAnnotation(JsonProperty.class);
                    if (!Objects.isNull(annotation)) {
                        String value = annotation.value();
                        map.put(value, declaredField);
                    } else {
                        String name = declaredField.getName();
                        map.put(name, declaredField);
                    }
                }
            }
        }
        return map;
    }
}
