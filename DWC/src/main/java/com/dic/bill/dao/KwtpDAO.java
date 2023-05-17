package com.dic.bill.dao;

import com.dic.bill.dto.SumPayment;
import com.dic.bill.model.scott.Kwtp;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;


public interface KwtpDAO extends JpaRepository<Kwtp, Integer> {

    /**
     * Получить платеж по № извещения (из ГИС ЖКХ)
     *
     * @param numDoc № извещения (из ГИС ЖКХ)
     */
    @Query(value = "select t from Kwtp t "
            + "where t.numDoc=:numDoc")
    Kwtp getByNumDoc(@Param("numDoc") String numDoc);

    @Query(value = "select t.dt as dt, coalesce(t.summa,0)+coalesce(t.penya,0) as summa, c.org.name as source from Kwtp t join t.comps c "
            + "where t.kart.koKw.id=:klskId " +
            "order by t.dt")
    List<SumPayment> getByKlskId(@Param("klskId") Long klskId);

}
