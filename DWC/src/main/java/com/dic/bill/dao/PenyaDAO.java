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
    @Query(value = "select distinct k from Kart k left join fetch k.penya p left join fetch k.eolink e where k.uk.id=:ukId")
    List<Kart> getKartWithDebitByReu(@Param("ukId") Integer ukId);

    /**
     * Получить лиц.счета, которые имеют задолженность или текущее начисление
     * @param grpDeb - группа организаций для объединения задолженности в один файл
     */
    @Query(value = "select distinct k from Kart k left join fetch k.penya p where k.uk.grpDeb=:grpDeb")
    List<Kart> getKartWithDebitByGrpDeb(@Param("grpDeb") Integer grpDeb);

    @Query(value= """
            select max(t.summa) as "debt", max(t.penya) as "pen", sum(case when c.type=0 then c.summa else 0 end) as "chrg",
                 sum(case when c.type=1 then c.summa else 0 end) as "pay", sum(case when c.type=1 then c.summap else 0 end) as "payPen", substr(d.mg,5,2)||'.'||substr(d.mg,1,4) as "mg"
                  from scott.kart k join scott.long_table d on d.mg >= ?2
                  left join scott.c_penya t on d.mg=t.mg1 and k.lsk=t.lsk
                  left join scott.c_chargepay2 c on d.mg=c.mg and k.lsk=c.lsk and ?3 between c.mgFrom and c.mgTo
                  where k.k_lsk_id=?1
            group by d.mg
            having max(t.summa)<>0 or  max(t.penya)<>0 or  sum(case when c.type=0 then c.summa else 0 end)<>0 or  sum(case when c.type=1 then c.summa else 0 end)<>0 or
            sum(case when c.type=1 then c.summap else 0 end) <>0
            order by d.mg""", nativeQuery = true)
    List<SumFinanceFlow> getFinanceFlowByKlskSincePeriod(Long klskId, String periodBack, String period);

}
