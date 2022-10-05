package com.dic.app.mm.impl.bot;

import com.dic.bill.dto.SumFinanceFlow;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

public class BotReportBase {

    public static final String MONEY_PATTERN = "###,###,###.##";

    protected void setMaxColumSize(List<?> lst, Map<String, Column> columns, Class<?> clasz) throws IntrospectionException, IllegalAccessException, InvocationTargetException {
        BeanInfo beanInfo = Introspector.getBeanInfo(clasz);

        for (Object row : lst) {
            for (PropertyDescriptor propertyDesc : beanInfo.getPropertyDescriptors()) {
                String propertyName = propertyDesc.getName();
                Class<?> propertyType = propertyDesc.getPropertyType();
                if (propertyType.equals(Double.class)) {
                    Double value = (Double) propertyDesc.getReadMethod().invoke(row);
                    putFieldSize(columns, propertyName, value);
                } else if (propertyType.equals(String.class)) {
                    String value = (String) propertyDesc.getReadMethod().invoke(row);
                    putFieldSize(columns, propertyName, value);
                }
            }
        }
    }

    private void putFieldSize(Map<String, Column> columns, String fieldName, Double inputDouble) {
        if (inputDouble != null) {
            DecimalFormat df = new DecimalFormat(BotReportBase.MONEY_PATTERN);
            String formatted = df.format(inputDouble);
            columns.computeIfPresent(fieldName, (key, val) -> {
                val.size = val.size < formatted.length() ? formatted.length() : val.size;
                return val;
            });
        }
    }

    private void putFieldSize(Map<String, Column> columns, String fieldName, String inputStr) {
        if (inputStr != null) {
            columns.computeIfPresent(fieldName, (key, val) -> {
                val.size = val.size < inputStr.length() ? inputStr.length() : val.size;
                return val;
            });
        }
    }

}
