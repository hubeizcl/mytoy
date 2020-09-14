package com.mytoy.starter.tools.validate.validated;

import com.mytoy.starter.tools.validate.NotBlankAfterValidateStringMatchPattern;
import org.apache.commons.lang.StringUtils;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.regex.Pattern;

/**
 * @author zhangchenglong8
 */
public class NotBlankAfterValidatedStringMatchPattern implements ConstraintValidator<NotBlankAfterValidateStringMatchPattern, String> {
    private String regPattern;


    @Override
    public void initialize(NotBlankAfterValidateStringMatchPattern constraintAnnotation) {
        regPattern = constraintAnnotation.regPattern();
    }

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (StringUtils.isNotBlank(value)) {
            if (StringUtils.isBlank(regPattern) || !Pattern.compile(regPattern).matcher(value).find()) {
                return false;
            }
        }
        return true;
    }
}
