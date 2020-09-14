package com.mytoy.starter.tools.validate;

import com.mytoy.starter.tools.validate.validated.NotBlankAfterValidatedStringMatchPattern;

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
@Constraint(validatedBy = NotBlankAfterValidatedStringMatchPattern.class)
public @interface NotBlankAfterValidateStringMatchPattern {

    String regPattern() ;

    String message() default "字符串不满足正则表达式！";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
