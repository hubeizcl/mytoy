package com.mytoy.starter.tools.validate.validated;

import com.mytoy.starter.tools.validate.NotBlankAfterValidateDateTime;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * @author zhangchenglong8
 * 这个校验需要搭配@NotBlank注解一起使用
 */
public class NotBlankAfterValidatedDateTime implements ConstraintValidator<NotBlankAfterValidateDateTime, String> {

    private static final Logger logger = LoggerFactory.getLogger(NotBlankAfterValidatedDateTime.class);
    private String[] dateTimePatterns;


    @Override
    public void initialize(NotBlankAfterValidateDateTime constraintAnnotation) {
        dateTimePatterns = constraintAnnotation.dateTimePatterns();

    }

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (StringUtils.isNotBlank(value) && ArrayUtils.isNotEmpty(dateTimePatterns)) {
            for (String dateTimePattern : dateTimePatterns) {
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimePattern, Locale.CHINA);
                    LocalDateTime parse = LocalDateTime.parse(value, formatter);
                    if (null != parse) {
                        return true;
                    }
                } catch (Exception e) {

                }
            }
            return false;//全部未通过校验，就返回失败
        }
        return true;
    }
}
