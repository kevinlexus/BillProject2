package com.dic.bill.dao;

import com.dic.bill.model.scott.Org;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

/**
 * DAO сущности com.dic.bill.model.scott.Org
 */
public interface OrgDAO extends JpaRepository<Org, Integer> {


    /**
     * Получить организацию по коду REU
     *
     * @param reu код REU
     */
    @Query("select t from com.dic.bill.model.scott.Org t where t.reu = ?1")
    Org getByReu(String reu);

    @Query("select t from com.dic.bill.model.scott.Org t where t.reu is not null")
    List<Org> getAllUk();

    /**
     * Получить организацию по CD
     *
     * @param cd код CD
     */
    @Query("select t from com.dic.bill.model.scott.Org t where t.cd = ?1")
    Org getByCD(String cd);

    /**
     * Получить организацию (наименование города) по Типу орг
     *
     * @param tp тип организации
     */
    @Query("select t from com.dic.bill.model.scott.Org t where t.orgTp.cd = ?1")
    Org getByOrgTp(String tp);

    List<Org> findByIsExchangeExt(boolean isExchangeExt);
}
