package com.dic.bill.dao;

import com.dic.bill.dto.KartExtPaymentRec;
import com.dic.bill.dto.KartExtPaymentRec2;
import com.dic.bill.dto.SumPayment;
import com.dic.bill.model.scott.Akwtp;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;


public interface AkwtpDAO extends JpaRepository<Akwtp, Integer> {

    /**
     * Получить платеж по № извещения (из ГИС ЖКХ)
     * @param numDoc - № извещения (из ГИС ЖКХ)
     * @return
     */
    @Query(value = "select t from Akwtp t "
            + "where t.numDoc=:numDoc and t.mg between :mgFrom and :mgTo")
    Akwtp getByNumDoc(@Param("numDoc") String numDoc,
                      @Param("mgFrom") String mgFrom, @Param("mgTo") String mgTo);


    /**
     * Получить платежи по внешним лиц.счетам, используя FK_KLSK_ID и FK_KLSK_PREMISE (для ЧГК)
     */
    @Query(value = "select t.kwtp_id as id, t.dtek as dt, coalesce(e.ext_lsk, e2.ext_lsk) as extLsk, t.summa " +
            "from scott.a_kwtp_day t join scott.kart k on t.lsk=k.lsk\n" +
            "left join scott.kart_ext e on e.fk_klsk_premise=k.fk_klsk_premise\n" +
            "left join scott.kart_ext e2 on e2.fk_klsk_id=k.k_lsk_id\n" +
            "and exists (select * from scott.kart_ext e where e.fk_klsk_premise=k.fk_klsk_premise or e.fk_klsk_id=k.k_lsk_id)\n" +
            "where t.dtek between :dt1 and :dt2 and t.mg = to_char(:dt1, 'YYYYMM')\n" +
            "and t.org=:orgId\n" +
            "and t.summa is not null\n" +
            "and t.priznak = 1\n" +
            "and coalesce(e.ext_lsk, e2.ext_lsk) is not null\n" +
            "order by coalesce(e.ext_lsk, e2.ext_lsk) ", nativeQuery = true)
    List<KartExtPaymentRec> getPaymentByPeriodUsingKlskId(@Param("dt1") Date dt1, @Param("dt2") Date dt2, @Param("orgId") Integer orgId);

    /**
     * Получить платежи по внешним лиц.счетам, используя lsk (для ФКР)
     */
    @Query(value = "select e.ext_lsk as extLsk, t.dtek as dt, h.guid||', '||ltrim(k.kw,'0') as fiasKw,\n" +
            "                   t.summa*100 as summa, \n" +
            "                   e.raschet_schet as rsch, o2.name||', '||s.name||', '||ltrim(lower(k.nd),'0')||', '||ltrim(lower(k.kw),'0') as adr\n" +
            "                        from scott.a_kwtp t join scott.kart k on t.lsk=k.lsk\n" +
            "                        join scott.kart_ext e on e.lsk=k.lsk\n" +
            "                        join scott.spul s on k.kul=s.id\n" +
            "                        join scott.c_houses h on k.house_id=h.id\n" +
            "                        join scott.t_org_tp tp2 on tp2.cd='Город'\n" +
            "                        join scott.t_org o2 on o2.fk_orgtp=tp2.id\n" +
            "                        where exists (select * from scott.kart_ext e where e.lsk=k.lsk)\n" +
            "                        and t.dtek between :dt1 and :dt2\n" +
            "                        and t.mg = to_char(:dt1, 'YYYYMM')\n" +
            "                        and e.fk_uk=:orgId\n" +
            "                        order by e.ext_lsk", nativeQuery = true)
    List<KartExtPaymentRec2> getPaymentByPeriodUsingLsk(@Param("dt1") Date dt1, @Param("dt2") Date dt2, @Param("orgId") Integer orgId);

    @Query(value = "select t.dt as dt, t.summa as summa, c.org.name as source from Akwtp t join t.comps c "
            + "where t.kart.koKw.id=:klskId and t.mg between :periodFrom and :periodTo " +
            "order by t.dt")
    List<SumPayment> getByKlskIdPeriod(@Param("klskId") Long klskId, @Param("periodFrom") String periodFrom,
                                       @Param("periodTo") String periodTo);
}
