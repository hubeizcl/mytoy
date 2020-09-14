package com.mytoy.starter.tools.objectconvert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.mytoy.starter.tools.MyArrays;
import com.mytoy.starter.tools.MyMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author zhangchenglong8
 */
public class ObjectCopyUtils {
    private final static Logger logger = LoggerFactory.getLogger(ObjectCopyUtils.class);
    private final static String basicType4Byte = byte.class.getTypeName();
    private final static String basicType4Short = short.class.getTypeName();
    private final static String basicType4Int = int.class.getTypeName();
    private final static String basicType4Long = long.class.getTypeName();
    private final static String basicType4Float = float.class.getTypeName();
    private final static String basicType4Double = double.class.getTypeName();
    private final static String basicType4Boolean = boolean.class.getTypeName();
    private final static String basicType4Char = char.class.getTypeName();
    private final static String packagingType4Byte = Byte.class.getTypeName();
    private final static String packagingType4Short = Short.class.getTypeName();
    private final static String packagingType4Integer = Integer.class.getTypeName();
    private final static String packagingType4Long = Long.class.getTypeName();
    private final static String packagingType4Float = Float.class.getTypeName();
    private final static String packagingType4Double = Double.class.getTypeName();
    private final static String packagingType4Boolean = Boolean.class.getTypeName();
    private final static String packagingType4Character = Character.class.getTypeName();
    private final static String packagingType4BigDecimal = BigDecimal.class.getTypeName();
    private final static String basicType4String = String.class.getTypeName();
    private final static List<String> basicTypeList4Byte = ImmutableList.of(basicType4Byte, packagingType4Byte);
    private final static List<String> basicTypeList4Short = ImmutableList.of(basicType4Short, packagingType4Short);
    private final static List<String> basicTypeList4Int = ImmutableList.of(basicType4Int, packagingType4Integer);
    private final static List<String> basicTypeList4Long = ImmutableList.of(basicType4Long, packagingType4Long);
    private final static List<String> basicTypeList4Float = ImmutableList.of(basicType4Float, packagingType4Float);
    private final static List<String> basicTypeList4Double = ImmutableList.of(basicType4Double, packagingType4Double);
    private final static List<String> basicTypeList4Boolean = ImmutableList.of(basicType4Boolean, packagingType4Boolean);
    private final static List<String> basicTypeList4Char = ImmutableList.of(basicType4Char, packagingType4Character);

    private final static List<String> basicTypeList = Arrays.asList(basicType4Byte, packagingType4Byte,
            basicType4Short, packagingType4Short,
            basicType4Int, packagingType4Integer,
            basicType4Long, packagingType4Long,
            basicType4Float, packagingType4Float,
            basicType4Double, packagingType4Double);

    public static List<String> numType4List() {
        List<String> list = Lists.newArrayList(basicTypeList);
        list.add(packagingType4BigDecimal);
        return list;
    }

    public static String getBasicType4String() {
        return basicType4String;
    }


    public static <T> T deepClone(T srcObj) {
        if (null == srcObj) return null;
        try {
            Class<?> srcObjClass = srcObj.getClass();
            T dstObj = (T) srcObjClass.newInstance();
            if (null != dstObj)
                return deepClone(srcObj, dstObj, ArrayUtils.EMPTY_STRING_ARRAY, ArrayUtils.EMPTY_STRING_ARRAY);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /**
     * 適用於源對象於目標對象屬性名稱不一致的情況,使用字段映射来赋值
     */
    private static <E, T> T deepClone(E srcObj, T dstObj, Map<String, String> fieldNameMapping) {
        if (null == srcObj || null == dstObj || MyMap.isEmpty(fieldNameMapping)) return dstObj;
        Class<?> srcObjClass = srcObj.getClass();
        Class<?> dstObjClass = dstObj.getClass();
        fieldNameMapping.entrySet().forEach(entry -> {
            String includeSrcFieldName = entry.getKey();
            String includeDstFieldName = entry.getValue();
            try {
                Field srcObjClassDeclaredField = srcObjClass.getDeclaredField(includeSrcFieldName);
                Field dstObjClassDeclaredField = dstObjClass.getDeclaredField(includeDstFieldName);
                if (!Modifier.isFinal(srcObjClassDeclaredField.getModifiers()) && !Modifier.isFinal(dstObjClassDeclaredField.getModifiers())) {
                    srcObjClassDeclaredField.setAccessible(true);
                    dstObjClassDeclaredField.setAccessible(true);
                    String srcObjClassDeclaredFieldTypeName = srcObjClassDeclaredField.getType().getTypeName();
                    String dstObjClassDeclaredFieldTypeName = dstObjClassDeclaredField.getType().getTypeName();
                    if (StringUtils.equals(srcObjClassDeclaredFieldTypeName, dstObjClassDeclaredFieldTypeName)) {
                        dstObjClassDeclaredField.set(dstObj, srcObjClassDeclaredField.get(srcObj));
                    } else {
                        packaging(dstObj, dstObjClassDeclaredField, srcObjClassDeclaredFieldTypeName, dstObjClassDeclaredFieldTypeName, srcObjClassDeclaredField.get(srcObj));
                        Object value = srcObjClassDeclaredField.get(srcObj);
                        if ((StringUtils.equals(ObjectCopyUtils.getBasicType4String(), srcObjClassDeclaredFieldTypeName) && ObjectCopyUtils.numType4List().contains(dstObjClassDeclaredFieldTypeName))
                                || (StringUtils.equals(ObjectCopyUtils.getBasicType4String(), dstObjClassDeclaredFieldTypeName) && ObjectCopyUtils.numType4List().contains(srcObjClassDeclaredFieldTypeName))) {
                            StringConversion(dstObj, dstObjClassDeclaredField, srcObjClassDeclaredFieldTypeName, dstObjClassDeclaredFieldTypeName, value);//基本类型，包装类，BigDecimal和String之间的自动转换，當然能轉換的前提是，能夠將String解析成相應的值
                        }
                    }
                }
            } catch (Exception e) {
            }
        });
        return dstObj;
    }


    /**
     * 适用于源对象和目标对象属性名称一致的情况
     */
    private static <E, T> T deepClone(E srcObj, T dstObj, String[] includeField, String[] excludeField) {
        if (null == srcObj || null == dstObj) return dstObj;
        Class<?> srcObjClass = srcObj.getClass();
        Class<?> dstObjClass = dstObj.getClass();
        List<String> includeFieldList = new ArrayList<>();
        List<String> excludeFieldList = new ArrayList<>();
        if (MyArrays.isNotEmpty(includeField) && MyArrays.isNotEmpty(excludeField))
            includeFieldList = Arrays.asList(includeField);
        else if (MyArrays.isNotEmpty(includeField)) includeFieldList = Arrays.asList(includeField);
        else excludeFieldList = Arrays.asList(excludeField);
        Field[] srcObjClassFields = srcObjClass.getDeclaredFields();
        if (null != srcObjClassFields && srcObjClassFields.length > 0) {
            for (Field srcObjClassField : srcObjClassFields) {
                srcObjClassField.setAccessible(true);
                String fieldName = srcObjClassField.getName();
                if (excludeFieldList.contains(fieldName)) continue;//排除的属性跳过循环
                if (includeFieldList.contains(fieldName) || includeFieldList.isEmpty()) {
                    try {
                        Field dstObjClassField = dstObjClass.getDeclaredField(fieldName);
                        if (Modifier.isFinal(dstObjClassField.getModifiers()))
                            continue;//如果目标对象属性是final,因为是属于类的属性，不能修改，跳过
                        String srcObjClassFieldTypeName = srcObjClassField.getType().getTypeName();
                        String dstObjClassFieldTypeName = dstObjClassField.getType().getTypeName();
                        dstObjClassField.setAccessible(true);
                        if (StringUtils.equals(srcObjClassFieldTypeName, dstObjClassFieldTypeName)) {
                            dstObjClassField.set(dstObj, srcObjClassField.get(srcObj));
                            continue;
                        }
                        if (ObjectCopyUtils.numType4List().contains(srcObjClassFieldTypeName) && ObjectCopyUtils.numType4List().contains(dstObjClassFieldTypeName)) {
                            packaging(dstObj, dstObjClassField, srcObjClassFieldTypeName, dstObjClassFieldTypeName, srcObjClassField.get(srcObj));
                        } else {
                            //这个地方我想了半天，到底有没有必要递归把同名但是不同类型的变量的全部属性拿到后转换给目标对象的属性，想了半天，应该是否定的
                            //因为，如果只是名称一样，那么将拿到很多不必要的属性，所以如果有这方面的需求，可以使用Map2ObjectUtils的deepClone方法，具体就是先把
                            //源对象转成json，再转map，然后调用方法
                            //Object srcObjectFieldObj = srcObjClassField.get(srcObj);
                            //dstObjClassField.set(dstObj, deepClone(srcObjectFieldObj, dstObjClassField.getType().newInstance(), ArrayUtils.EMPTY_STRING_ARRAY, ArrayUtils.EMPTY_STRING_ARRAY));//递归调防止获取对象的对象
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            }
        }
        return dstObj;
    }


    /**
     * 模仿对于基本类型数据，包装类型的自动装箱和拆箱类型数据，增加BigDecimal和基本類型，包裝類的轉換方法
     *
     * @param dstObj
     * @param dstObjClassDeclaredField
     * @param srcObjClassDeclaredFieldTypeName
     * @param dstObjClassDeclaredFieldTypeName
     * @param value
     * @throws IllegalAccessException
     */
    public static <T> void packaging(T dstObj, Field dstObjClassDeclaredField, String srcObjClassDeclaredFieldTypeName, String dstObjClassDeclaredFieldTypeName, Object value) throws IllegalAccessException {
        if (null != basicType4Byte && null != packagingType4Byte && basicTypeList4Byte.contains(srcObjClassDeclaredFieldTypeName) && basicTypeList4Byte.contains(dstObjClassDeclaredFieldTypeName)) {
            if (StringUtils.equals(basicType4Byte, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Byte.valueOf((byte) value));
            } else {
                dstObjClassDeclaredField.set(dstObj, (byte) value);
            }
            return;
        }
        if (null != basicType4Short && null != packagingType4Short && basicTypeList4Short.contains(srcObjClassDeclaredFieldTypeName) && basicTypeList4Short.contains(dstObjClassDeclaredFieldTypeName)) {
            if (StringUtils.equals(basicType4Short, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Short.valueOf((short) value));
            } else {
                dstObjClassDeclaredField.set(dstObj, (short) value);
            }
            return;
        }
        if (null != basicType4Int && null != packagingType4Integer && basicTypeList4Int.contains(srcObjClassDeclaredFieldTypeName) && basicTypeList4Int.contains(dstObjClassDeclaredFieldTypeName)) {
            if (StringUtils.equals(basicType4Int, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Integer.valueOf((int) value));
            } else {
                dstObjClassDeclaredField.set(dstObj, (int) value);
            }
            return;
        }
        if (null != basicType4Long && null != packagingType4Long && basicTypeList4Long.contains(srcObjClassDeclaredFieldTypeName) && basicTypeList4Long.contains(dstObjClassDeclaredFieldTypeName)) {
            if (StringUtils.equals(basicType4Long, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Long.valueOf((long) value));
            } else {
                dstObjClassDeclaredField.set(dstObj, (long) value);
            }
            return;
        }
        if (null != basicType4Float && null != packagingType4Float && basicTypeList4Float.contains(srcObjClassDeclaredFieldTypeName) && basicTypeList4Float.contains(dstObjClassDeclaredFieldTypeName)) {
            if (StringUtils.equals(basicType4Float, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Float.valueOf((float) value));
            } else {
                dstObjClassDeclaredField.set(dstObj, (float) value);
            }
            return;
        }
        if (null != basicType4Double && null != packagingType4Double && basicTypeList4Double.contains(srcObjClassDeclaredFieldTypeName) && basicTypeList4Double.contains(dstObjClassDeclaredFieldTypeName)) {
            if (StringUtils.equals(basicType4Double, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Double.valueOf((double) value));
            } else {
                dstObjClassDeclaredField.set(dstObj, (double) value);
            }
            return;
        }
        if (null != basicType4Boolean && null != packagingType4Boolean && basicTypeList4Boolean.contains(srcObjClassDeclaredFieldTypeName) && basicTypeList4Boolean.contains(dstObjClassDeclaredFieldTypeName)) {
            if (StringUtils.equals(basicType4Boolean, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Boolean.valueOf((boolean) value));
            } else {
                dstObjClassDeclaredField.set(dstObj, (boolean) value);
            }
            return;
        }
        if (null != basicType4Char && null != packagingType4Character && basicTypeList4Char.contains(srcObjClassDeclaredFieldTypeName) && basicTypeList4Char.contains(dstObjClassDeclaredFieldTypeName)) {
            if (StringUtils.equals(basicType4Char, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Character.valueOf((char) value));
            } else {
                dstObjClassDeclaredField.set(dstObj, (char) value);
            }
            return;
        }
        if ((basicTypeList.contains(srcObjClassDeclaredFieldTypeName) && StringUtils.equals(packagingType4BigDecimal, dstObjClassDeclaredFieldTypeName))) {
            if (basicTypeList4Long.contains(srcObjClassDeclaredFieldTypeName)
                    || basicTypeList4Int.contains(srcObjClassDeclaredFieldTypeName)
                    || basicTypeList4Short.contains(srcObjClassDeclaredFieldTypeName)
                    || basicTypeList4Byte.contains(srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, BigDecimal.valueOf((long) value));//byte,short和int可以强转转换成long
                return;
            }
            if (basicTypeList4Double.contains(srcObjClassDeclaredFieldTypeName) || basicType4Float.contains(srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, BigDecimal.valueOf((double) value));//float可以强转为double
                return;
            }
        }
        if ((basicTypeList.contains(dstObjClassDeclaredFieldTypeName) && StringUtils.equals(packagingType4BigDecimal, srcObjClassDeclaredFieldTypeName))) {
            if (StringUtils.equals(basicType4Float, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((BigDecimal) value).floatValue());
                return;
            }
            if (StringUtils.equals(packagingType4Float, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Float.valueOf(((BigDecimal) value).floatValue()));
                return;
            }
            if (StringUtils.equals(basicType4Double, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((BigDecimal) value).doubleValue());
                return;
            }
            if (StringUtils.equals(packagingType4Double, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Double.valueOf(((BigDecimal) value).doubleValue()));
                return;
            }
            if (StringUtils.equals(basicType4Byte, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((BigDecimal) value).byteValue());
                return;
            }
            if (StringUtils.equals(packagingType4Byte, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Byte.valueOf(((BigDecimal) value).byteValue()));
                return;
            }
            if (StringUtils.equals(basicType4Short, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((BigDecimal) value).shortValue());
                return;
            }
            if (StringUtils.equals(packagingType4Short, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Short.valueOf(((BigDecimal) value).shortValue()));
                return;
            }
            if (StringUtils.equals(basicType4Int, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((BigDecimal) value).intValue());
                return;
            }
            if (StringUtils.equals(packagingType4Integer, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Integer.valueOf(((BigDecimal) value).intValue()));
                return;
            }
            if (StringUtils.equals(basicType4Long, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((BigDecimal) value).longValue());
                return;
            }
            if (StringUtils.equals(packagingType4Long, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Long.valueOf(((BigDecimal) value).longValue()));
            }
        }
    }

    /**
     * 基本类型，包装类，BigDecimal和String之间的转换
     *
     * @param dstObj
     * @param dstObjClassDeclaredField
     * @param srcObjClassDeclaredFieldTypeName
     * @param dstObjClassDeclaredFieldTypeName
     * @param value
     * @throws Exception
     */
    public static <T> void StringConversion(T dstObj, Field dstObjClassDeclaredField, String srcObjClassDeclaredFieldTypeName, String dstObjClassDeclaredFieldTypeName, Object value) throws Exception {
        String numRegPattern = "^(\\-|\\+)?\\d+(\\.\\d+)?$";
        Pattern compile = Pattern.compile(numRegPattern);
        if (StringUtils.equals(basicType4String, srcObjClassDeclaredFieldTypeName)) {
            if (!compile.matcher((String) value).find()) return;
            if (StringUtils.equals(basicType4Float, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Float.valueOf((String) value).floatValue());
                return;
            }
            if (StringUtils.equals(packagingType4Float, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Float.valueOf((String) value));
                return;
            }
            if (StringUtils.equals(basicType4Double, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Double.valueOf((String) value).doubleValue());
                return;
            }
            if (StringUtils.equals(packagingType4Double, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Double.valueOf((String) value));
                return;
            }
            if (StringUtils.equals(basicType4Byte, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Byte.valueOf((String) value).byteValue());
                return;
            }
            if (StringUtils.equals(packagingType4Byte, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Byte.valueOf((String) value));
                return;
            }
            if (StringUtils.equals(basicType4Short, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Short.valueOf((String) value).shortValue());
                return;
            }
            if (StringUtils.equals(packagingType4Short, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Short.valueOf((String) value));
                return;
            }
            if (StringUtils.equals(basicType4Int, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Integer.valueOf((String) value).intValue());
                return;
            }
            if (StringUtils.equals(packagingType4Integer, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Integer.valueOf((String) value));
                return;
            }
            if (StringUtils.equals(basicType4Long, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Long.valueOf((String) value).longValue());
                return;
            }
            if (StringUtils.equals(packagingType4Long, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Long.valueOf((String) value));
            }
            if (StringUtils.equals(packagingType4BigDecimal, dstObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, new BigDecimal((String) value));
            }
        } else {
            if (StringUtils.equals(basicType4Float, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Float.valueOf((float) value).toString());
                return;
            }
            if (StringUtils.equals(packagingType4Float, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((Float) value).toString());
                return;
            }
            if (StringUtils.equals(basicType4Double, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Double.valueOf((double) value).toString());
                return;
            }
            if (StringUtils.equals(packagingType4Double, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((Double) value).toString());
                return;
            }
            if (StringUtils.equals(basicType4Byte, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Byte.valueOf((byte) value).toString());
                return;
            }
            if (StringUtils.equals(packagingType4Byte, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((Byte) value).toString());
                return;
            }
            if (StringUtils.equals(basicType4Short, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Short.valueOf((short) value).toString());
                return;
            }
            if (StringUtils.equals(packagingType4Short, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((Short) value).toString());
                return;
            }
            if (StringUtils.equals(basicType4Int, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Integer.valueOf((int) value).toString());
                return;
            }
            if (StringUtils.equals(packagingType4Integer, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((Integer) value).toString());
                return;
            }
            if (StringUtils.equals(basicType4Long, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, Long.valueOf((long) value).toString());
                return;
            }
            if (StringUtils.equals(packagingType4Long, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((Long) value).toString());
            }
            if (StringUtils.equals(packagingType4BigDecimal, srcObjClassDeclaredFieldTypeName)) {
                dstObjClassDeclaredField.set(dstObj, ((BigDecimal) value).toString());
            }
        }
    }
}
