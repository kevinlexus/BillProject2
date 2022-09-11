package com.dic.bill.dao;

import com.dic.bill.dto.SumFinanceFlow;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Penya;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;


public interface PenyaDAO extends JpaRepository<Penya, Integer> {


    /**
     * Удалить текущую пеню
     * @param lsk - лиц.счет
     */
    @Modifying
    @Query(value = "delete from Penya t where t.kart.lsk=:lsk")
    void deleteByLsk(@Param("lsk") String lsk);

    /**
     * Получить все элементы по lsk
     *
     * @param lsk - лиц.счет
     */
    @Query("select t from Penya t "
            + "where t.kart.id = :lsk and nvl(t.summa,0) <> 0 "
            + "order by t.mg1")
    List<Penya> getByLsk(@Param("lsk") String lsk);

    /**
     * Получить лиц.счета, которые имеют задолженность или текущее начисление
     * @param ukId - Id УК
     */
    @Query(value = "select distinct k from Kart k left join fetch k.penya p where k.uk.id=:ukId")
    List<Kart> getKartWithDebitByReu(@Param("ukId") Integer ukId);

    /**
     * Получить лиц.счета, которые имеют задолженность или текущее начисление
     * @param grpDeb - группа организаций для объединения задолженности в один файл
     */
    @Query(value = "select distinct k from Kart k left join fetch k.penya p where k.uk.grpDeb=:grpDeb")
    List<Kart> getKartWithDebitByGrpDeb(@Param("grpDeb") Integer grpDeb);


/*
   @Query(value="select sum(t.summa) as \"debt\", sum(t.penya) as \"pn\", sum(c.summa) as \"chrg\",\n" +
           "     sum(c2.summa) as \"pay\", sum(c2.summap) as \"payPen\", t.mg as \"mg\"\n" +
           "      from scott.kart k join scott.long_table t on  t.mg >= ?2\n" +
           "      left join scott.c_penya t on t.mg=t.mg1 and k.lsk=t.lsk\n" +
           "      left join scott.c_chargepay2 c on t.mg=c.mg and k.lsk=c.lsk and c.type=0 and ?2 between c.mgFrom and c.mgTo\n" +
           "      left join scott.c_chargepay2 c2 on t.mg=c2.mg and k.lsk=c2.lsk and c2.type=1 and ?2 between c2.mgFrom and c2.mgTo\n" +
           "      where k.k_lsk_id=?1\n" +
           "group by t.mg\n" +
           "having coalesce(sum(t.summa), sum(t.penya), sum(c.summa), sum(c2.summa), sum(c2.summap)) <>0\n" +
           "order by t.mg", nativeQuery = true)
    List<SumFinanceFlow> getFinanceFlowByKlskSincePeriod(Long klskId, String period);
*/

   @Query(value="select sum(t.summa) as \"debt\", 111.23 as \"pn\", 1137734.66 as \"chrg\",\n" +
           "     34455445.56 as \"pay\", 983.33 as \"payPen\", t.mg as \"mg\"\n" +
           "      from scott.kart k join scott.long_table t on  t.mg >= ?2\n" +
           "      left join scott.c_penya t on t.mg=t.mg1 and k.lsk=t.lsk\n" +
           "      left join scott.c_chargepay2 c on t.mg=c.mg and k.lsk=c.lsk and c.type=0 and ?2 between c.mgFrom and c.mgTo\n" +
           "      left join scott.c_chargepay2 c2 on t.mg=c2.mg and k.lsk=c2.lsk and c2.type=1 and ?2 between c2.mgFrom and c2.mgTo\n" +
           "      where k.k_lsk_id=?1\n" +
           "group by t.mg\n" +
           "having coalesce(sum(t.summa), sum(t.penya), sum(c.summa), sum(c2.summa), sum(c2.summap)) <>0\n" +
           "order by t.mg", nativeQuery = true)
    List<SumFinanceFlow> getFinanceFlowByKlskSincePeriod(Long klskId, String period);
}
