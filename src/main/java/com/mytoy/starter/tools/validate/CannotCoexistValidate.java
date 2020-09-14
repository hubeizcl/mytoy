package com.mytoy.starter.tools.validate;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 同时存在的变量个数，按组校验
 */
@Documented
@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER, TYPE_USE})
@Retention(RUNTIME)
public @interface CannotCoexistValidate {
    /**
     * 同时存在的参数个数，默认只能一个,注意如果改变默认值，那么最后只取最大的那个值
     *
     * @return
     */
    int exist() default 1;

    /**
     * 参数中多组不能同时存在的参数，第一组是1，第二组是2，以此类推
     *
     * @return
     */
    int group() default 1;


}
