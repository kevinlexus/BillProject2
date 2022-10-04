package com.dic.app.service.impl;

import com.dic.bill.dto.cursor.AllTabColumns;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class RegistryMapper implements RowMapper<Object[]> {

    private final List<AllTabColumns> fields;
    private final boolean convertToOem;

    public RegistryMapper(List<AllTabColumns> fields, boolean convertToOem) {
        this.fields = fields;
        this.convertToOem = convertToOem;
    }

    @Override
    public Object[] mapRow(ResultSet resultSet, int i) throws SQLException {
        Object[] rowData = new Object[fields.size()];
        int j = 0;
        for (AllTabColumns field : fields) {
            log.info("check1={}", field.getColumnName());
            log.info("check2={}", resultSet.getString(field.getColumnName()));
            switch (field.getDataType()) {
                case "CHAR", "VARCHAR2" -> {
                    String val = resultSet.getString(field.getColumnName());
                    if (val != null) {
                        if (convertToOem) {
                            String cp866 = val;
                            //String cp866 = new String(val.getBytes(Charset.forName("cp866")));
                            rowData[j] = cp866;
                        } else {
                            rowData[j] = val;
                        }
                    }
                }
                case "NUMBER" -> rowData[j] = resultSet.getFloat(field.getColumnName());
                case "DATE" -> rowData[j] = resultSet.getDate(field.getColumnName());
                default -> throw new RuntimeException("Неподдерживаемый тип данных " + field.getDataType());
            }
            j++;
        }
        return rowData;
    }
}
