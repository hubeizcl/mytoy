package com.mytoy.starter.tools.validate.validated;

import com.mytoy.starter.tools.validate.NotBlankAfterValidateStringInArray;
import org.apache.commons.lang.StringUtils;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Arrays;

/**
 * @author zhangchenglong8
 */
public class NotBlankAfterValidatedStringInArray implements ConstraintValidator<NotBlankAfterValidateStringInArray, String> {

    private String[] paramCheck;


    @Override
    public void initialize(NotBlankAfterValidateStringInArray constraintAnnotation) {
        paramCheck = constraintAnnotation.paramCheck();
    }

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (StringUtils.isNotBlank(value)) {
            if ((null == paramCheck || paramCheck.length == 0) || !(Arrays.asList(paramCheck).contains(value))) {
              return false;
            }
        }
        return true;
    }
}
