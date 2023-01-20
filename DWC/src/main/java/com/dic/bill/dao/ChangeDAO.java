package com.dic.bill.dao;

import com.dic.bill.dto.SumChangeRec;
import com.dic.bill.dto.SumRecMgDt;
import com.dic.bill.dto.SumUslOrgRec;
import com.dic.bill.model.scott.Change;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import javax.persistence.QueryHint;
import java.util.List;


public interface ChangeDAO extends JpaRepository<Change, Integer> {

    /**
     * Получить сгруппированные записи перерасчетов текущего периода
     * @param lsk - лицевой счет
     */
    @Query(value = "select t.usl as uslId, t.org as orgId, sum(t.summa) as summa "
            + "from SCOTT.V_CHANGES_FOR_SALDO t "
            + "where t.lsk=:lsk "
            + "and nvl(t.summa,0) <> 0 "
            + "group by t.usl, t.org",
            nativeQuery = true)
    List<SumUslOrgRec> getChangeByLskGrouped(@Param("lsk") String lsk);


    /**
     * Получить сгруппированные записи перерасчетов текущего периода
     * @param lsk - лицевой счет
     */
    @Query(value = "select t.mgchange as mg, coalesce(sum(t.summa),0) as summa, t.dt as dt "
            + "from Change t "
            + "where t.kart.lsk=:lsk "
            + "group by t.mgchange, t.dt")
    @QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
    List<SumRecMgDt> getChangeByPeriodAndLsk(@Param("lsk") String lsk);

    /**
     * Получить сгруппированные записи перерасчетов текущего периода
     */
    @Query(value = "select t.kart.spul.name as street, t.kart.nd as nd, t.usl.nm2 as nm, max(t.usl.npp) as npp, " +
            "coalesce(sum(t.summa),0) as summa "
            + "from Change t where t.changeDoc.id=:id "
            + "group by t.kart.spul.name, t.kart.nd, t.usl.nm2 "
            + "order by t.kart.spul.name, t.kart.nd, max(t.usl.npp)")
    List<SumChangeRec> getChangeById(@Param("id") Integer id);

}
