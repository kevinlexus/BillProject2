package com.dic.bill.mm.impl;

import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.TuserDAO;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.MeterData;
import com.dic.bill.dto.UslMeterDateVol;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.*;
import com.ric.cmn.CommonErrs;
import com.ric.cmn.MeterValConsts;
import com.ric.cmn.Utl;
import com.ric.dto.MapMeter;
import com.ric.dto.MeterVal;
import com.ric.dto.projection.SumMeterVol;
import com.ric.dto.SumMeterVolExt;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.procedure.ProcedureOutputs;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class MeterMngImpl implements MeterMng {

    private final MeterDAO meterDao;
    private final TuserDAO tuserDAO;

    @PersistenceContext
    private EntityManager em;


    /**
     * Получить первый попавшийся актуальный счетчик по параметрам
     *
     * @param ko  - Ko объекта, где установлен счетчик
     * @param usl - код услуги
     * @param dt  - дата на которую получить
     * @return
     */

    @Override
    public Optional<Meter> getActualMeterByKoUsl(Ko ko, String usl, Date dt) {
        return meterDao.findActualByKoUsl(ko.getId(), usl, dt).stream().findFirst();
    }

    /**
     * Получить первый попавшийся актуальный счетчик по помещению
     *
     * @param ko  - Ko помещения., где установлен счетчик
     * @param usl - код услуги
     * @param dt  - дата на которую получить
     */

    @Override
    public Optional<Meter> getActualMeterByKo(Ko ko, String usl, Date dt) {
        // список уникальных фин.лиц. к которым привязаны счетчики
        List<Ko> lstKoFinLsk = ko.getKartByPremise().stream().map(Kart::getKoKw).distinct().collect(Collectors.toList());
        for (Ko koFinLsk : lstKoFinLsk) {
            // найти первый попавшийся счетчик по всем фин.лиц. в помещении
            Optional<Meter> actualMeter = getActualMeterByKoUsl(koFinLsk, usl, dt);
            if (actualMeter.isPresent()) {
                return actualMeter;
            }
        }
        // не найдено
        return Optional.empty();
    }

    /**
     * Получить объем в доле одного дня по счетчикам помещения
     *
     * @param lstMeterVol -  объемы по счетчикам
     * @param calcStore   - хранилище необходимых данных для расчета пени, начисления
     * @return - объем в доле 1 дня к периоду наличия рабочего счетчика
     */
    public List<UslMeterDateVol> getPartDayMeterVol(List<SumMeterVol> lstMeterVol, CalcStore calcStore) {
        Calendar c = Calendar.getInstance();
        // distinct список кодов услуг найденных счетчиков
        List<Usl> lstMeterUsl = lstMeterVol.stream()
                .map(t -> em.find(Usl.class, t.getUslId())).distinct().collect(Collectors.toList());
        List<UslMeterDateVol> lstMeterDateVol = new ArrayList<>();

        // перебрать услуги
        for (Usl usl : lstMeterUsl) {
            int workDays = 0;
            // сумма объема по всем счетчикам данной услуги
            BigDecimal vol = lstMeterVol.stream().filter(t -> t.getUslId().equals(usl.getId()))
                    .map(SumMeterVol::getVol)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            // перебрать дни текущего месяца
            BigDecimal diff = vol;
            UslMeterDateVol lastUslMeterDateVol = null;
            // подсчитать кол-во дней работы счетчика
            for (c.setTime(calcStore.getCurDt1()); !c.getTime().after(calcStore.getCurDt2());
                 c.add(Calendar.DATE, 1)) {
                Date curDt = c.getTime();
                // найти любой действующий счетчик, прибавить день
                SumMeterVol meterVol = lstMeterVol.stream().filter(t -> t.getUslId().equals(usl.getId()) &&
                        Utl.between(curDt, t.getDtFrom(), t.getDtTo())).findFirst().orElse(null);
                if (meterVol != null) {
                    // прибавить день
                    workDays++;
                }
            }

            for (c.setTime(calcStore.getCurDt1()); !c.getTime().after(calcStore.getCurDt2());
                 c.add(Calendar.DATE, 1)) {
                Date curDt = c.getTime();
                // найти любой действующий счетчик, прибавить день
                SumMeterVol meterVol = lstMeterVol.stream().filter(t -> t.getUslId().equals(usl.getId()) &&
                        Utl.between(curDt, t.getDtFrom(), t.getDtTo())).findFirst().orElse(null);
                if (meterVol != null) {
                    // доля объема на 1 рабочий день наличия счетчика
                    BigDecimal partDayVol = vol.divide(BigDecimal.valueOf(workDays), 5, RoundingMode.HALF_UP);
                    UslMeterDateVol uslMeterDateVol = new UslMeterDateVol(usl, curDt, partDayVol);
                    lastUslMeterDateVol = uslMeterDateVol;
                    lstMeterDateVol.add(uslMeterDateVol);
                    diff = diff.subtract(partDayVol);
                }
            }
            // округление
            if (lastUslMeterDateVol != null && diff.compareTo(BigDecimal.ZERO) != 0) {
                lastUslMeterDateVol.vol = lastUslMeterDateVol.vol.add(diff);
            }
        }
        return lstMeterDateVol;
    }

    /**
     * Узнать, работал ли хоть один счетчик в данном дне
     *
     * @param uslId - код услуги
     * @param dt    - дата на которую проверить
     * @return
     */
    @Override
    public boolean isExistAnyMeter(List<SumMeterVol> lstMeterVol, String uslId, Date dt) {
        SumMeterVol meterVol = lstMeterVol.stream().filter(t -> t.getUslId().equals(uslId) &&
                Utl.between(dt, t.getDtFrom(), t.getDtTo())).findFirst().orElse(null);
        return meterVol != null;
    }

    /**
     * Проверить наличие в списке показания по счетчику
     *
     * @param lst  - список показаний
     * @param guid - GUID прибора учета
     * @param ts   - временная метка
     * @return
     */
    @Override
    public boolean getIsMeterDataExist(List<MeterData> lst, String guid, XMLGregorianCalendar ts) {
        Date dt = Utl.truncDateToSeconds(Utl.getDateFromXmlGregCal(ts));
        MeterData meterData = lst.stream().filter(t -> t.getGuid().equals(guid) && t.getTs().compareTo(dt) == 0)
                .findFirst().orElse(null);
        return meterData != null;
    }


    /**
     * Проверить, является ли счетчик в Директ актуальным //note удалить!
     *
     * @param meter - счетчик
     * @param dt    - проверочная дата
     * @return
     */
/*
    @Override
    public boolean getIsMeterActual(Meter meter, Date dt) {
        return Utl.between(dt, meter.getDt1(), meter.getDt2());
    }
*/

    /**
     * Проверить, открыт ли счетчик в Директ принятия показаний от ГИС
     *
     * @param meter - счетчик
     * @return
     */
    @Override
    public boolean getIsMeterOpenForReceiveData(Meter meter) {
        if (meter.getGisConnTp() == null) {
            return false;
        } else if (meter.getGisConnTp().equals(1) || meter.getGisConnTp().equals(3)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Проверить, открыт ли счетчик в Директ для отправки показаний от ГИС
     *
     * @param meter - счетчик
     * @return
     */
    @Override
    public boolean getIsMeterOpenForSendData(Meter meter) {
        if (meter.getGisConnTp() == null) {
            return false;
        } else if (meter.getGisConnTp().equals(2) || meter.getGisConnTp().equals(3)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Проверить, возможно ли сохранить показания по счетчику в Директ
     *
     * @param meterEol - счетчик Eolink
     * @param dt       - проверочная дата
     */
    @Override
    public boolean getCanSaveDataMeter(Eolink meterEol, Date dt) {
        Ko ko = meterEol.getKoObj();
        Meter meter = ko.getMeter();
        return meter.getIsMeterActual(dt) && getIsMeterOpenForReceiveData(meter);
    }

    /**
     * Получить показания по счетчику со статусом, по периоду
     *
     * @param meter  - счетчик
     * @param status - статус
     * @param period - период
     */
    @Override
    public List<ObjPar> getValuesByMeter(Meter meter, int status, String period) {
        return meter.getKo().getObjPar().stream()
                .filter(t -> t.getLst().getCd().equals("ins_sch") && t.getMg().equals(period)
                        && t.getStatus().equals(status))
                .sorted(Comparator.comparing(ObjPar::getId).reversed())
                .collect(Collectors.toList());
    }

    /**
     * ТЕСТОВЫЙ МЕТОД - ПРОВЕРЯЛ LockModeType.PESSIMISTIC_READ
     *
     * @param i
     * @param i1
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<Meter> findMeter(int i, int i1) throws InterruptedException {
        List<Meter> lst = meterDao.findMeter(72802, 72805);
        lst.forEach(t -> {
            log.info("Meter.id={} n1={}", t.getId(), t.getN1());
        });
        Thread.sleep(15000);
        lst.forEach(t -> {
            log.info("Meter.id={} n1={}", t.getId(), t.getN1());
        });
        Thread.sleep(15000);
        lst.forEach(t -> {
            log.info("Meter.id={} n1={}", t.getId(), t.getN1());
        });
        Thread.sleep(15000);
        lst.forEach(t -> {
            log.info("Meter.id={} n1={}", t.getId(), t.getN1());
        });
        Thread.sleep(15000);
        lst.forEach(t -> {
            log.info("Meter.id={} n1={}", t.getId(), t.getN1());
        });
        return lst;
    }

    /**
     * Сохранить показания по счетчику в биллинг
     *  @param writer           объект записи лога
     * @param lsk              лиц.счет
     * @param strUsl           код услуги
     * @param prevValue        предыдущее показание
     * @param curValue         текущее показание
     * @param docParId         Id записи реестра
     * @param isSetPreviousVal установить предыдущее показание? ВНИМАНИЕ! Текущие введёные показания будут сброшены назад
     */
    @Override
    public int saveMeterValByRow(BufferedWriter writer, String lsk, String strUsl,
                                 String prevValue, String curValue,
                                 int userId, int docParId,
                                 boolean isSetPreviousVal) throws IOException {
        if (strUsl != null && strUsl.length() >= 3 && !strUsl.equals("Нет счетчика")) {
            String codeUsl = strUsl.substring(0, 3);
            Double curVal = 0D;
            Double prevVal = 0D;
            if (curValue != null && curValue.length() > 0) {
                curVal = Double.valueOf(curValue);
            }
            if (prevValue != null && prevValue.length() > 0) {
                prevVal = Double.valueOf(prevValue);
            }

            log.trace("Сохранение по лиц={}, услуге usl={}, пред.показание ={}, показание ={}",
                    lsk, codeUsl, prevVal, curVal);

            Integer ret = saveMeterValByLsk(new MeterVal(lsk, userId, docParId, isSetPreviousVal, codeUsl, curVal, prevVal));

            log.trace("Результат исполнения scott.p_meter.ins_data_meter={}", ret);
            String mess = " По лиц p_lsk=%s, услуге p_usl=%s, предыдущ.показ p_prev_val=%f, показание p_cur_val=%f, период p_period=%s";
            if (ret.equals(-1)) {
                String str = String.format(CommonErrs.ERR_MSG_METER_VAL_OTHERS +
                        mess, lsk, codeUsl, prevVal, curVal);
                writer.write(str + "\r\n");
            } else if (ret.equals(1)) {
                // нулевые показания - не считать за ошибку
                //String str = String.format(CommonErrs.ERR_MSG_METER_VAL_ZERO +
                //        mess, lsk, codeUsl, prevVal, curVal, period);
                //writer.write(str + "\r\n");
            } else if (ret.equals(3)) {
                // показания меньше или равны существующим - не считать за ошибку
                //String str = String.format(CommonErrs.ERR_MSG_METER_VAL_LESS +
                //        mess, lsk, codeUsl, prevVal, curVal, period);
                //writer.write(str + "\r\n");
            } else if (ret.equals(4)) {
                String str = String.format(CommonErrs.ERR_MSG_METER_NOT_FOUND +
                        mess, lsk, codeUsl, prevVal, curVal);
                writer.write(str + "\r\n");
            } else if (ret.equals(5)) {
                String str = String.format(CommonErrs.ERR_MSG_METER_VAL_TOO_BIG +
                        mess, lsk, codeUsl, prevVal, curVal);
                writer.write(str + "\r\n");
            } else {
                String str = String.format(CommonErrs.ERR_MSG_METER_VAL_SUCCESS +
                        mess, lsk, codeUsl, prevVal, curVal);
                log.trace(str);
            }
            return ret;


        }
        return 0; // нет данных для отправки
    }

    /**
     * Сохранить показание счетчика в БД по лиц.счету
     *
     * @param meterVal
     * @return 0-успешно сохранено
     *         3-показания меньше или равны предыдущим
     *         4-не найден активный счетчик (на конец месяца)
     *         5-превышено значение
     */
    private Integer saveMeterValByLsk(MeterVal meterVal) {
        StoredProcedureQuery query = em
                .createStoredProcedureQuery(
                        "scott.p_meter.ins_data_meter")
                .registerStoredProcedureParameter(
                        "p_lsk",
                        String.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_usl",
                        String.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_prev_val",
                        Double.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_cur_val",
                        Double.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_is_set_prev",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_ts",
                        Date.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_status",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_user",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_doc_par_id",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_ret",
                        Integer.class,
                        ParameterMode.OUT
                )
                .setParameter("p_lsk", meterVal.getLsk())
                .setParameter("p_usl", meterVal.getCodeUsl())
                .setParameter("p_prev_val", meterVal.getPrevVal())
                .setParameter("p_cur_val", meterVal.getCurVal())
                .setParameter("p_is_set_prev", meterVal.isSetPreviousVal() ? 1 : 0)
                .setParameter("p_ts", new Date())
                .setParameter("p_status", MeterValConsts.INSERT_FOR_LOAD_TO_GIS)
                .setParameter("p_doc_par_id", meterVal.getDocParId())
                .setParameter("p_user", meterVal.getUserId());
        Integer ret;
        try {
            query.execute();

            ret = (Integer) query
                    .getOutputParameterValue("p_ret");

        } finally {
            query.unwrap(ProcedureOutputs.class).release();
        }
        return ret;
    }

    /**
     * Сохранить показание счетчика в БД по Id счетчика
     *
     *
     * @param meterId Id счетчика
     * @param curVal текущее показание
     * @return 0-успешно сохранено
     *         3-показания меньше или равны предыдущим
     *         4-не найден активный счетчик (на конец месяца)
     *         5-превышено значение
     */
    @Override
    @Transactional
    public Integer saveMeterValByMeterId(int meterId, double curVal) {
        StoredProcedureQuery query = em
                .createStoredProcedureQuery(
                        "scott.p_meter.ins_data_meter")
                .registerStoredProcedureParameter(
                        "p_meter_id",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_n1",
                        Double.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_ts",
                        Date.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_status",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_user",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_control",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_is_set_prev",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_doc_par_id",
                        Integer.class,
                        ParameterMode.IN
                )
                .registerStoredProcedureParameter(
                        "p_ret",
                        Integer.class,
                        ParameterMode.OUT
                )
                .setParameter("p_meter_id", meterId)
                .setParameter("p_n1", curVal)
                .setParameter("p_is_set_prev", 0)
                .setParameter("p_ts", new Date())
                .setParameter("p_status", MeterValConsts.INSERT_FOR_LOAD_TO_GIS)
                .setParameter("p_doc_par_id", null)
                .setParameter("p_user", tuserDAO.findByCd("TLG").getId());
        Integer ret;
        try {
            query.execute();

            ret = (Integer) query
                    .getOutputParameterValue("p_ret");

        } finally {
            query.unwrap(ProcedureOutputs.class).release();
        }
        return ret;
    }

    @Override
    public MapMeter getMapMeterByKlskId(Long koObjId, Date dt1, Date dt2) {
        List<SumMeterVolExt> lstMeter = meterDao.getMeterVolExtByKlskId(koObjId, dt1, dt2);
        return new MapMeter(lstMeter.stream().collect(Collectors.toMap(SumMeterVolExt::getMeterId, v -> v)));
    }

}