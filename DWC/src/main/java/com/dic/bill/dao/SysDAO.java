package com.dic.bill.dao;

import com.dic.bill.dto.cursor.AllTabColumns;
import com.dic.bill.dto.cursor.DebitsLskMonth;
import com.dic.bill.model.scott.Usl;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;


public interface SysDAO extends JpaRepository<Usl, String> {

    @Query(value = "select t.column_name as columnname, t.data_type as datatype, " +
            " coalesce(t.data_precision,t.data_length) as datalength, t.data_scale as datascale " +
            "         from all_tab_columns t where lower(t.table_name)=:tableName " +
            "         order by t.column_id ", nativeQuery = true)
    List<AllTabColumns> getAllTabColumns(@Param("tableName") String tableName);


}
