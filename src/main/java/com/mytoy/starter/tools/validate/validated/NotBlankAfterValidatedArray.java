package com.mytoy.starter.tools.validate.validated;

import com.mytoy.starter.tools.validate.NotBlankAfterValidateArray;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Arrays;

/**
 * @author zhangchenglong8
 */
public class NotBlankAfterValidatedArray implements ConstraintValidator<NotBlankAfterValidateArray, String[]> {

    private int capacity;

    private int capacityGte;

    private int capacityLte;

    private String[] paramCheck;


    @Override
    public void initialize(NotBlankAfterValidateArray constraintAnnotation) {
        capacity = constraintAnnotation.capacity();
        paramCheck = constraintAnnotation.paramCheck();
        capacityGte = constraintAnnotation.capacityGte();
        capacityLte = constraintAnnotation.capacityLte();
    }

    @Override
    public boolean isValid(String[] value, ConstraintValidatorContext context) {
        if (null != value && value.length > 0) {
            boolean flag1 = false;
            boolean flag2 = false;
            boolean flag3 = false;
            boolean flag4 = false;
            if ((null == paramCheck || paramCheck.length == 0) || (Arrays.asList(paramCheck).containsAll(Arrays.asList(value)))) {
                flag1 = true;
            }
            if (0 == capacity || value.length == capacity) {
                flag2 = true;
            }
            if (0 == capacityLte || value.length <= capacityLte) {
                flag3 = true;
            }
            if (Integer.MAX_VALUE == capacityGte || value.length >= capacityGte) {
                flag4 = true;
            }
            if (flag1 && flag2 && flag3 && flag4) {
                return true;
            }
        }
        return false;
    }
}
