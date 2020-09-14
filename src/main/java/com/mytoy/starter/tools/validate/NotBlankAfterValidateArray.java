package com.mytoy.starter.tools.validate;

import com.mytoy.starter.tools.validate.validated.NotBlankAfterValidatedArray;

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
@Constraint(validatedBy = NotBlankAfterValidatedArray.class)
public @interface NotBlankAfterValidateArray {

    String[] paramCheck() default {};

    int capacityGte() default Integer.MAX_VALUE;

    int capacityLte() default 0;

    int capacity() default 0;

    String message() default "集合不满足校验条件！";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
