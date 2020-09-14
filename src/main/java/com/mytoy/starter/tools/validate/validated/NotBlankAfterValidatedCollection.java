package com.mytoy.starter.tools.validate.validated;

import com.mytoy.starter.tools.validate.NotBlankAfterValidateCollection;
import org.apache.commons.collections4.CollectionUtils;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author zhangchenglong8
 */
public class NotBlankAfterValidatedCollection implements ConstraintValidator<NotBlankAfterValidateCollection, Collection<Object>> {

    private int capacity;

    private int capacityGte;

    private int capacityLte;

    private String[] paramCheck;


    @Override
    public void initialize(NotBlankAfterValidateCollection constraintAnnotation) {
        capacity = constraintAnnotation.capacity();
        paramCheck = constraintAnnotation.paramCheck();
        capacityGte = constraintAnnotation.capacityGte();
        capacityLte = constraintAnnotation.capacityLte();
    }

    @Override
    public boolean isValid(Collection<Object> value, ConstraintValidatorContext context) {
        if (CollectionUtils.isNotEmpty(value)) {
            boolean flag1 = false;
            boolean flag2 = false;
            boolean flag3 = false;
            boolean flag4 = false;
            if ((null == paramCheck || paramCheck.length == 0) || (Arrays.asList(paramCheck).containsAll(Arrays.asList(value)))) {
                flag1 = true;
            }
            if (0 == capacity || value.size() == capacity) {
                flag2 = true;
            }
            if (0 == capacityLte || value.size() <= capacityLte) {
                flag3 = true;
            }
            if (Integer.MAX_VALUE == capacityGte || value.size() >= capacityGte) {
                flag4 = true;
            }
            if (flag1 && flag2 && flag3 && flag4) {
                return true;
            }
        }
        return false;
    }

}
