package com.dic.bill.dao;

import com.dic.bill.dto.MeterData;
import com.dic.bill.dto.MeterValue;
import com.ric.dto.SumMeterVol;
import com.dic.bill.model.scott.Meter;
import com.ric.dto.SumMeterVolExt;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import javax.persistence.LockModeType;
import javax.persistence.QueryHint;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * DAO сущности Meter
 *
 * @author Lev
 * @version 1.00
 */
@Repository()
public interface MeterDAO extends JpaRepository<Meter, Integer> {


    /**
     * Получить все актуальные счетчики по объекту Ko, коду услуги
     *
     * @param koId  - объект Ko, к которому прикреплен счетчик
     * @param uslId - код услуги
     */
    @Query(value = "select t from Meter t "
            + "where t.koObj.id = ?1 and t.usl.id = ?2 " +
            "and ?3 between t.dt1 and t.dt2")
    List<Meter> findActualByKoUsl(Long koId, String uslId, Date dt);

    /**
     * Получить актуальный счетчик по klskId
     *
     * @param klskId klskId объекта, к которому прикреплен счетчик
     * @param dt   дата актуальности
     */
    @Query(value = "select t from Meter t "
            + "where t.ko.id = ?1 " +
            "and ?2 between t.dt1 and t.dt2")
    Optional<Meter> getActualByKlskId(Long klskId, Date dt);

    /**
     * Получить все актуальные счетчики по объекту Ko, по действующим, основным лиц.счетам
     * отсортированные по адресу
     * @param reu код УК
     * @param dt  дата актуальности
     */
    @Query(value = "select t from Meter t join t.koObj o join o.kart k "
            + "where k.uk.reu = ?1 " +
            "and ?2 between t.dt1 and t.dt2 " +
            "and k.psch not in (8,9) and k.tp.cd='LSK_TP_MAIN'" +
            "order by k.kartDetail.ord1")
    List<Meter> findActualByReuOrderedByAdress(String reu, Date dt);

    /**
     * Получить суммарный объем по счетчикам всех услуг, в объекте koObj за период
     *
     * @param koObjId - Klsk объекта, к которому привязан счетчик
     * @param dtFrom  - начало периода
     * @param dtTo    - оконачание периода
     */
    @QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
    @Query(value = "select t.id as meterId, t.usl.id as uslId, t.dt1 as dtFrom, t.dt2 as dtTo, " +
            "coalesce(sum(o.n1),0) as vol, coalesce(t.n1,0) as n1, t.usl.name as serviceName " +
            "from Meter t " +
            "left join t.objPar o with o.lst.cd='ins_vol_sch' and o.mg = TO_CHAR(?2,'YYYYMM') "
            + "where t.koObj.id = ?1 " +
            "and ((?2 between t.dt1 and t.dt2 or ?3 between t.dt1 and t.dt2) or " +
            "(t.dt1 between ?2 and ?3 or t.dt2 between ?2 and ?3)) " +
            "group by t.id, t.usl.id, t.dt1, t.dt2, t.n1, t.usl.name ")
    List<SumMeterVol> getMeterVolByKlskId(Long koObjId, Date dtFrom, Date dtTo);

    /**
     * Получить Timestamp показаний и GUID счетчиков, по которым они были приняты
     *
     * @param userCd - CD внёсшего пользователя
     * @param period - период
     * @param lstCd  - тип действия
     */
    @Query(value = "select t.ts as ts, t.ko.eolink.guid as guid from ObjPar t "
            + "where t.tuser.cd = ?1 and t.lst.cd=?2 and t.mg=?3")
    List<MeterData> findMeteringDataTsUsingUser(String userCd, String lstCd, String period);

    /**
     * ТЕСТОВЫЙ МЕТОД - ПРОВЕРЯЛ LockModeType.PESSIMISTIC_READ
     */
    @Lock(LockModeType.PESSIMISTIC_READ)
    @Query(value = "select t from Meter t "
            + "where t.id between ?1 and ?2")
    List<Meter> findMeter(int n1, int n2);

    @QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
    @Query(value = "select new com.ric.dto.SumMeterVolExt(t.id, t.usl.id, t.dt1, t.dt2, " +
            "coalesce(sum(o.n1),0), coalesce(t.n1,0), t.usl.nameForBot) " +
            "from Meter t " +
            "left join t.objPar o with o.mg = TO_CHAR(?2,'YYYYMM') and o.lst.cd='ins_vol_sch' "
            + "where t.koObj.id = ?1 " +
            "and ((?2 between t.dt1 and t.dt2 or ?3 between t.dt1 and t.dt2) or " +
            "(t.dt1 between ?2 and ?3 or t.dt2 between ?2 and ?3)) " +
            "group by t.id, t.usl.id, t.dt1, t.dt2, t.n1, t.usl.nameForBot")
    List<SumMeterVolExt> getMeterVolExtByKlskId(Long koObjId, Date dtFrom, Date dtTo);


    /**
     * Получить показания по счетчикам, не отправленные в ГИС, по дому todo - сделать по помещениям, не входящим в подъезд!
     */
    @Query(value = """
            select t.id, t.n1, t.dt_crt as dtCrt, e.guid, e.id as eolinkId
                                   from t_objxpar t
                                            join u_list u on t.fk_list = u.id and u.cd = 'ins_sch'
                                            join meter m on t.fk_k_lsk = m.k_lsk_id and m.gis_conn_tp in (2,3) -- счетчик подключен для отправки показаний в ГИС
                                            join exs.eolink e on m.k_lsk_id = e.fk_klsk_obj -- счетчик
                                            join exs.eolink e2 on e.parent_id=e2.id -- помещение, входящее в подъезд
                                            join exs.eolink e3 on e2.parent_id=e3.id -- подъезд
                                            join exs.eolink e4 on e3.parent_id=e4.id and e4.guid=:houseGuid -- дом
                                   where t.status in (:statuses)
                                     and t.mg = :period 
                                     and t.n1 != 0
                                     order by e.guid, t.id"""
            , nativeQuery = true
    )
    List<MeterValue> getHouseMeterValue(@Param("houseGuid") String houseGuid, @Param("period") String period, @Param("statuses") List<Integer> statuses);

}

