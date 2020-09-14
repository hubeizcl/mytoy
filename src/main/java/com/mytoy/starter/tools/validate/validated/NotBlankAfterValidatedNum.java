package com.mytoy.starter.tools.validate.validated;

import com.mytoy.starter.tools.validate.NotBlankAfterValidateNum;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * @author zhangchenglong8
 */
public class NotBlankAfterValidatedNum implements ConstraintValidator<NotBlankAfterValidateNum, Integer> {

    private int max;

    private int min;

    @Override
    public boolean isValid(Integer value, ConstraintValidatorContext context) {
        if (null != value) {
            boolean flag1 = false;
            boolean flag2 = false;
            if (value <= max) {
                flag1 = true;
            }
            if (value >= min) {
                flag2 = true;
            }
            if (flag1 && flag2) {
                return true;
            }
            return false;
        }
        return true;

    }

    @Override
    public void initialize(NotBlankAfterValidateNum constraintAnnotation) {
        max = constraintAnnotation.max();
        min = constraintAnnotation.min();

    }
}
