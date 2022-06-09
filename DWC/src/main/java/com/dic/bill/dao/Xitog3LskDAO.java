package com.dic.bill.dao;

import com.dic.bill.model.scott.Xitog3Lsk;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

/**
 * DAO сущности Xitog3Lsk
 *
 * @author Lev
 * @version 1.00
 */
@Repository()
public interface Xitog3LskDAO extends JpaRepository<Xitog3Lsk, Integer> {

    /**
     * Получить текущую пеню
     *
     * @param lsk лиц.счет
     * @param mg  период
     */
    @Query(value = "select sum(t.pcur) from Xitog3Lsk t "
            + "where t.kart.lsk = :lsk and t.mg = :mg")
    BigDecimal getPenCurPeriod(@Param("lsk") String lsk, @Param("mg") String mg);

}
