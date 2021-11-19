package com.dic.bill.dao;

import com.dic.bill.dto.KartLsk;
import com.dic.bill.dto.LskCharge;
import com.dic.bill.dto.LskNabor;
import com.dic.bill.model.scott.Kart;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import javax.persistence.QueryHint;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

public interface KartDAO extends JpaRepository<Kart, String> {

    @Query("select t from Kart t where t.kul = :kul and t.nd=:nd and t.num=:kw")
    List<Kart> findByKulNdKw(@Param("kul") String kul, @Param("nd") String nd, @Param("kw") String kw);

    // закомментировал psch not in (8,9) - не пересчитываются полностью закрытые помещения (закрыты все лиц.счета) ред. 13.03.2019
    @Query(value = "select distinct t.k_lsk_id from SCOTT.KART t where t.reu=:reuId /*and t.PSCH not in (8,9)*/ order by t.k_lsk_id",
            nativeQuery = true)
    List<BigDecimal> findAllKlskIdByReuId(@Param("reuId") String reuId);

    @Query(value = "select distinct t.k_lsk_id from SCOTT.KART t, SCOTT.C_VVOD d where d.house_id=t.house_id " +
            "and d.house_id=:houseId /*and t.PSCH not in (8,9)*/ order by t.k_lsk_id", nativeQuery = true)
    List<BigDecimal> findAllKlskIdByHouseId(@Param("houseId") long houseId);

    @Query(value = "select distinct t.k_lsk_id from SCOTT.KART t where " +
            "t.kul||t.nd in (:kulNds) order by t.k_lsk_id", nativeQuery = true)
    List<BigDecimal> findAllKlskIdByKulNds(@Param("kulNds") List<String> kulNds);

    @Query(value = "select distinct t.k_lsk_id from SCOTT.KART t, SCOTT.C_VVOD d, SCOTT.NABOR n " +
            "where d.house_id=t.house_id " +
            "and d.id=:vvodId and t.lsk=n.lsk and n.usl=d.usl and n.FK_VVOD=d.id /*and t.PSCH not in (8,9)*/ " +
            "order by t.k_lsk_id", nativeQuery = true)
    List<BigDecimal> findAllKlskIdByVvodId(@Param("vvodId") long vvodId);

    @Query(value = "select distinct t.k_lsk_id from SCOTT.KART t /*where t.psch not in (8,9)*/ order by t.k_lsk_id", nativeQuery = true)
    List<BigDecimal> findAllKlskId();

    @Query("select t from Kart t where t.uk.reu=:reu and t.tp.cd=:tpCd " +
            "and t.house.id = :houseId and t.num=:kw and t.psch not in (8,9)")
    List<Kart> findActualByReuHouseIdTpKw(@Param("reu") String reu, @Param("tpCd") String tpCd,
                                          @Param("houseId") Integer houseId, @Param("kw") String kw);

    //@EntityGraph(attributePaths = {"kartDetail", "kartPr"})
//    @Query("select t.lsk as lsk, t.koKw.id as klskId, t.psch as psch, t.num as num from Kart t " +
//            "where t.house.id = :houseId and t.num=:kw")
//    List<KartLsk> findByHouseIdKw(@Param("houseId") Integer houseId, @Param("kw") String kw);

    @Query("select t.lsk as lsk, t.koKw.id as klskId, t.psch as psch, t.num as num from Kart t " +
            "where t.house.id = :houseId")
    @QueryHints(value = {@QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT")})
    List<KartLsk> findByHouseId(@Param("houseId") Integer houseId);

    @EntityGraph(attributePaths = {"kartDetail"})
    @Query("select t from Kart t")
    List<Kart> findAllForOrdering();

    @Query("select distinct t from Kart t join t.nabor n where t.tp.cd=:tpCd and n.usl.id=:uslId " +
            "and t.house.id = :houseId and t.num=:kw and t.psch not in (8,9)")
    List<Kart> findActualByUslHouseIdTpKw(@Param("tpCd") String tpCd,
                                          @Param("uslId") String uslId,
                                          @Param("houseId") Integer houseId, @Param("kw") String kw);

    @Query("select t from Kart t where t.uk.reu=:reu and t.tp.cd=:tpCd " +
            "and t.psch not in (8,9) and t.status.cd in (:statusLst) order by t.kartDetail.ord1")
    List<Kart> findActualByReuStatusOrderedByAddress(@Param("reu") String reu,
                                                     @Param("statusLst") List<String> statusLst, @Param("tpCd") String tpCd);

    @Query(value = "select k.k_lsk_id as klskId, k.lsk as lsk, p.period as mg, a.usl as uslId, a.org as orgId " +
            "from scott.kart k " +
            "left join scott.v_lsk_tp tp on k.fk_tp=tp.id " +
            "join scott.params p on 1=1 " +
            "left join scott.nabor a on k.lsk=a.lsk and to_date(p.period||'01', 'YYYYMMDD') between a.dt1 and a.dt2 " +
            "join scott.usl u on a.usl=u.usl " +
            "where k.kul||k.nd in (:kulNds) and a.usl in (:uslIds) " +
            "and (:psch = 0 or :psch=1 and k.psch in (8,9) " +
            "         or :psch=2 and k.psch not in (8,9)) " +
            "and (:woKpr=1 and k.kpr=0 or :woKpr=0) " +
            "and (:kran1 = 1 and k.kran1 <> 0 " +
            "  or :kran1 = 2 and coalesce(k.kran1,0) = 0 " +
            "  or :kran1 = 0) " +
            "and case when :lskTp=0 and tp.cd='LSK_TP_MAIN' then 1 " + // только основные лс
            "             when :lskTp=0 and tp.cd is null then 1 " + // считать основными лс, где не заполнено k.fk_tp (старые периоды)
            "             when :lskTp=1 and tp.cd='LSK_TP_ADDIT' then 1  " + // только дополнительные лс
            "             when :lskTp=2 then 1 " + // все лс
            "             else 0 end=1 " +
            "and exists (select * from scott.v_lsk_priority s2 where s2.k_lsk_id=k.k_lsk_id and " +
            "                          (:status = 0 or :status = s2.status) and " +
            "                           scott.c_changes.is_sel_lsk(:isSch, s2.psch, u.cd, s2.sch_el, :psch) = 1) ", nativeQuery = true)
    List<LskNabor> getNaborsByKulNd(@Param("status") int status, @Param("isSch") int isSch, @Param("psch") int psch,
                                    @Param("woKpr") int woKpr, @Param("kran1") int kran1,
                                    @Param("lskTp") int lskTp,
                                    @Param("kulNds") List<String> kulNds,
                                    @Param("uslIds") List<String> uslIds);

    @Query(value = "select k.k_lsk_id as klskId, k.lsk as lsk, p.period as mg, a.usl as uslId, a.org as orgId " +
            "from scott.kart k " +
            "left join scott.v_lsk_tp tp on k.fk_tp=tp.id " +
            "join scott.params p on 1=1 " +
            "left join scott.nabor a on k.lsk=a.lsk and to_date(p.period||'01', 'YYYYMMDD') between a.dt1 and a.dt2 " +
            "join scott.usl u on a.usl=u.usl " +
            "where k.k_lsk_id in (:klskIds) and a.usl in (:uslIds) " +
            "and (:psch = 0 or :psch=1 and k.psch in (8,9) " +
            "         or :psch=2 and k.psch not in (8,9)) " +
            "and (:woKpr=1 and k.kpr=0 or :woKpr=0) " +
            "and (:kran1 = 1 and k.kran1 <> 0 " +
            "  or :kran1 = 2 and coalesce(k.kran1,0) = 0 " +
            "  or :kran1 = 0) " +
            "and case when :lskTp=0 and tp.cd='LSK_TP_MAIN' then 1 " + // только основные лс
            "             when :lskTp=0 and tp.cd is null then 1 " + // считать основными лс, где не заполнено k.fk_tp (старые периоды)
            "             when :lskTp=1 and tp.cd='LSK_TP_ADDIT' then 1  " + // только дополнительные лс
            "             when :lskTp=2 then 1 " + // все лс
            "             else 0 end=1 " +
            "and exists (select * from scott.v_lsk_priority s2 where s2.k_lsk_id=k.k_lsk_id and " +
            "                          (:status = 0 or :status = s2.status) and " +
            "                           scott.c_changes.is_sel_lsk(:isSch, s2.psch, u.cd, s2.sch_el, :psch) = 1) ", nativeQuery = true)
    List<LskNabor> getNaborsByKlskIds(@Param("status") int status, @Param("isSch") int isSch, @Param("psch") int psch,
                                      @Param("woKpr") int woKpr, @Param("kran1") int kran1,
                                      @Param("lskTp") int lskTp,
                                      @Param("klskIds") Set<Long> klskIds,
                                      @Param("uslIds") List<String> uslIds);

    @Query(value = "select k.k_lsk_id as klskId, k.lsk as lsk, k.mg as mg, a.usl as uslId, a.org as orgId " +
            "from scott.arch_kart k " +
            "left join scott.v_lsk_tp tp on k.fk_tp=tp.id " +
            "left join scott.a_nabor2 a on k.lsk=a.lsk and k.mg between a.mgFrom and a.mgTo and to_date(k.mg||'01', 'YYYYMMDD') between a.dt1 and a.dt2 " +
            "join scott.usl u on a.usl=u.usl " +
            "where k.kul||k.nd in (:kulNds) and a.usl in (:uslIds) " +
            "and k.mg between :periodFrom and :periodTo " +
            "and (:psch = 0 or :psch=1 and k.psch in (8,9) " +
            "         or :psch=2 and k.psch not in (8,9)) " +
            "and (:woKpr=1 and k.kpr=0 or :woKpr=0) " +
            "and (:kran1 = 1 and k.kran1 <> 0 " +
            "  or :kran1 = 2 and coalesce(k.kran1,0) = 0 " +
            "  or :kran1 = 0) " +
            "and case when :lskTp=0 and tp.cd='LSK_TP_MAIN' then 1 " + // только основные лс
            "             when :lskTp=0 and tp.cd is null then 1 " + // считать основными лс, где не заполнено k.fk_tp (старые периоды)
            "             when :lskTp=1 and tp.cd='LSK_TP_ADDIT' then 1  " + // только дополнительные лс
            "             when :lskTp=2 then 1 " + // все лс
            "             else 0 end=1 " +
            "and exists (select * from scott.v_lsk_priority s2 where s2.k_lsk_id=k.k_lsk_id and " +
            "                          (:status = 0 or :status = s2.status) and " +
            "                           scott.c_changes.is_sel_lsk(:isSch, s2.psch, u.cd, s2.sch_el, :psch) = 1) ", nativeQuery = true)
    List<LskNabor> getArchNaborsByKulNd(@Param("status") int status, @Param("isSch") int isSch, @Param("psch") int psch,
                                        @Param("woKpr") int woKpr, @Param("kran1") int kran1,
                                        @Param("lskTp") int lskTp,
                                        @Param("periodFrom") String periodFrom,
                                        @Param("periodTo") String periodTo,
                                        @Param("kulNds") List<String> kulNds,
                                        @Param("uslIds") List<String> uslIds);

    @Query(value = "select k.k_lsk_id as klskId, k.lsk as lsk, k.mg as mg, a.usl as uslId, a.org as orgId " +
            "from scott.arch_kart k " +
            "left join scott.v_lsk_tp tp on k.fk_tp=tp.id " +
            "left join scott.a_nabor2 a on k.lsk=a.lsk and k.mg between a.mgFrom and a.mgTo and to_date(k.mg||'01', 'YYYYMMDD') between a.dt1 and a.dt2 " +
            "join scott.usl u on a.usl=u.usl " +
            "where k.k_lsk_id in (:klskIds) and a.usl in (:uslIds) " +
            "and k.mg between :periodFrom and :periodTo " +
            "and (:psch = 0 or :psch=1 and k.psch in (8,9) " +
            "         or :psch=2 and k.psch not in (8,9)) " +
            "and (:woKpr=1 and k.kpr=0 or :woKpr=0) " +
            "and (:kran1 = 1 and k.kran1 <> 0 " +
            "  or :kran1 = 2 and coalesce(k.kran1,0) = 0 " +
            "  or :kran1 = 0) " +
            "and case when :lskTp=0 and tp.cd='LSK_TP_MAIN' then 1 " + // только основные лс
            "             when :lskTp=0 and tp.cd is null then 1 " + // считать основными лс, где не заполнено k.fk_tp (старые периоды)
            "             when :lskTp=1 and tp.cd='LSK_TP_ADDIT' then 1  " + // только дополнительные лс
            "             when :lskTp=2 then 1 " + // все лс
            "             else 0 end=1 " +
            "and exists (select * from scott.v_lsk_priority s2 where s2.k_lsk_id=k.k_lsk_id and " +
            "                          (:status = 0 or :status = s2.status) and " +
            "                           scott.c_changes.is_sel_lsk(:isSch, s2.psch, u.cd, s2.sch_el, :psch) = 1) ", nativeQuery = true)
    List<LskNabor> getArchNaborsByKlskIds(@Param("status") int status, @Param("isSch") int isSch, @Param("psch") int psch,
                                        @Param("woKpr") int woKpr, @Param("kran1") int kran1,
                                        @Param("lskTp") int lskTp,
                                        @Param("periodFrom") String periodFrom,
                                        @Param("periodTo") String periodTo,
                                        @Param("klskIds") Set<Long> klskIds,
                                        @Param("uslIds") List<String> uslIds);

    @Query(value = "select k.k_lsk_id as klskId, k.lsk as lsk, k.mg as mg, t.usl as uslId, t.org as orgId, a.org as naborOrgId, t.summa as summa, t.test_opl as vol " +
            "from scott.arch_kart k " +
            "join scott.a_charge2 t on k.lsk=t.lsk and k.mg between t.mgFrom and t.mgTo " +
            "left join scott.v_lsk_tp tp on k.fk_tp=tp.id " +
            "left join scott.a_nabor2 a on k.lsk=a.lsk and t.usl=a.usl and k.mg between a.mgFrom and a.mgTo and to_date(k.mg||'01', 'YYYYMMDD') between a.dt1 and a.dt2 " +  // для старых архивных записей nabor, не имеющих разделения периодов dt1, dt2 в месяце
            "join scott.usl u on t.usl=u.usl " +
            "where t.type=1 and k.kul||k.nd in (:kulNds) and t.usl in (:uslIds) " +
            "and k.mg between :periodFrom and :periodTo and coalesce(t.summa,0) <> 0 " +
            "and (:psch = 0 or :psch=1 and k.psch in (8,9) " +
            "         or :psch=2 and k.psch not in (8,9)) " +
            "and (:woKpr=1 and k.kpr=0 or :woKpr=0) " +
            "and (:kran1 = 1 and k.kran1 <> 0 " +
            "  or :kran1 = 2 and coalesce(k.kran1,0) = 0 " +
            "  or :kran1 = 0) " +
            "and case when :lskTp=0 and tp.cd='LSK_TP_MAIN' then 1 " + // только основные лс
            "             when :lskTp=0 and tp.cd is null then 1 " + // считать основными лс, где не заполнено k.fk_tp (старые периоды)
            "             when :lskTp=1 and tp.cd='LSK_TP_ADDIT' then 1  " + // только дополнительные лс
            "             when :lskTp=2 then 1 " + // все лс
            "             else 0 end=1 " +
            "and exists (select * from scott.v_lsk_priority s2 where s2.k_lsk_id=k.k_lsk_id and " +
            "                          (:status = 0 or :status = s2.status) and " +
            "                           scott.c_changes.is_sel_lsk(:isSch, s2.psch, u.cd, s2.sch_el, :psch) = 1) ", nativeQuery = true)
    List<LskCharge> getArchChargesByKulNd(@Param("status") int status, @Param("isSch") int isSch, @Param("psch") int psch,
                                          @Param("woKpr") int woKpr, @Param("kran1") int kran1,
                                          @Param("lskTp") int lskTp,
                                          @Param("periodFrom") String periodFrom,
                                          @Param("periodTo") String periodTo,
                                          @Param("kulNds") List<String> kulNds,
                                          @Param("uslIds") List<String> uslIds);

    @Query(value = "select k.k_lsk_id as klskId, k.lsk as lsk, k.mg as mg, t.usl as uslId, t.org as orgId, a.org as naborOrgId, t.summa as summa, t.test_opl as vol " +
            "from scott.arch_kart k " +
            "join scott.a_charge2 t on k.lsk=t.lsk and k.mg between t.mgFrom and t.mgTo " +
            "left join scott.v_lsk_tp tp on k.fk_tp=tp.id " +
            "left join scott.a_nabor2 a on k.lsk=a.lsk and t.usl=a.usl and k.mg between a.mgFrom and a.mgTo and to_date(k.mg||'01', 'YYYYMMDD') between a.dt1 and a.dt2 " +  // для старых архивных записей nabor, не имеющих разделения периодов dt1, dt2 в месяце
            "join scott.usl u on t.usl=u.usl " +
            "where t.type=1 and k.k_lsk_id in (:klskIds) and t.usl in (:uslIds) " +
            "and k.mg between :periodFrom and :periodTo and coalesce(t.summa,0) <> 0 " +
            "and (:psch = 0 or :psch=1 and k.psch in (8,9) " +
            "         or :psch=2 and k.psch not in (8,9)) " +
            "and (:woKpr=1 and k.kpr=0 or :woKpr=0) " +
            "and (:kran1 = 1 and k.kran1 <> 0 " +
            "  or :kran1 = 2 and coalesce(k.kran1,0) = 0 " +
            "  or :kran1 = 0) " +
            "and case when :lskTp=0 and tp.cd='LSK_TP_MAIN' then 1 " + // только основные лс
            "             when :lskTp=0 and tp.cd is null then 1 " + // считать основными лс, где не заполнено k.fk_tp (старые периоды)
            "             when :lskTp=1 and tp.cd='LSK_TP_ADDIT' then 1  " + // только дополнительные лс
            "             when :lskTp=2 then 1 " + // все лс
            "             else 0 end=1 " +
            "and exists (select * from scott.v_lsk_priority s2 where s2.k_lsk_id=k.k_lsk_id and " +
            "                          (:status = 0 or :status = s2.status) and " +
            "                           scott.c_changes.is_sel_lsk(:isSch, s2.psch, u.cd, s2.sch_el, :psch) = 1) ", nativeQuery = true)
    List<LskCharge> getArchChargesByKlskIds(@Param("status") int status, @Param("isSch") int isSch, @Param("psch") int psch,
                                            @Param("woKpr") int woKpr, @Param("kran1") int kran1,
                                            @Param("lskTp") int lskTp,
                                            @Param("periodFrom") String periodFrom,
                                            @Param("periodTo") String periodTo,
                                            @Param("klskIds") Set<Long> klskIds,
                                            @Param("uslIds") List<String> uslIds);

    @Query(value = "select distinct t.k_lsk_id from scott.kart t where t.lsk in (:lsks)", nativeQuery = true)
    List<Long> findKlskIdByLsk(@Param("lsks") List<String> lsks);

}
