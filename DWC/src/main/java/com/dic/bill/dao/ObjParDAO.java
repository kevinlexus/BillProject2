package com.dic.bill.dao;

import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.ObjPar;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.List;

public interface ObjParDAO extends JpaRepository<ObjPar, Integer> {

    @Query("select t from ObjPar t where t.ko.id = :klskId and t.lst.cd = :cd")
    ObjPar getByKlskCd(@Param("klskId") Long klskId, @Param("cd") String cd);

    @Query("select t.ko from ObjPar t join fetch t.ko.kart where t.lst.cd = :cd and t.s1 = :s1")
    List<Ko> getKoByObjPar(@Param("cd") String cd, @Param("s1") String s1);

    @Query("select t from ObjPar t where t.lst.cd = :cd")
    List<ObjPar> getAllByCd(@Param("cd") String cd);
}
