package com.mytoy.starter.tools.validate;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 此注解可以用来修饰String，Date，LocalDateTime这三种类型的成员变量
 */
@Documented
@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER, TYPE_USE})
@Retention(RUNTIME)
public @interface NotBlankAfterTimeIntervalValidate {
    /**
     * 时间格式，如果字段為String類型的，則需要填寫此字段，Date和localDateTime則不需要
     *
     * @return
     */
    String pattern() default "yyyy-MM-dd HH:mm:ss";

    /**
     * 时间顺序，在前的数字小，在后的数字大
     *
     * @return
     */
    int order();

    /**
     * 時間間隔，多少秒，默認0表示沒有時間間隔，即使用此注解标注的字段的时间是相等的
     *
     * @return
     */
    int interval() default 0;

    /**
     * 比较的时间是大于小于还是等于上一个时间间隔，默认等于，-1是小于，1是大于
     *
     * @return
     */
    int gtLtOrEq() default 0;

    /**
     * 参数中多组不能同时存在的参数，第一组是1，第二组是2，以此类推
     *
     * @return
     */
    int group() default 1;

}

