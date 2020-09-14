package com.mytoy.starter.tools.validate;

import com.mytoy.starter.tools.validate.validated.NotBlankAfterValidatedNum;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author zhangchenglong8
 */
@Documented
@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER, TYPE_USE})
@Retention(RUNTIME)
@Constraint(validatedBy = NotBlankAfterValidatedNum.class)
public @interface NotBlankAfterValidateNum {

    int min() default Integer.MIN_VALUE;

    int max() default Integer.MAX_VALUE;

    String message() default "数字不满足校验条件！";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};


}
