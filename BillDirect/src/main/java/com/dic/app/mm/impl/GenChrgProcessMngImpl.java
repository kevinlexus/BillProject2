package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.GenChrgProcessMng;
import com.dic.app.mm.GenPart;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dto.*;
import com.dic.bill.mm.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.projection.SumMeterVol;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервис расчета начисления
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
public class GenChrgProcessMngImpl implements GenChrgProcessMng {

    private final NaborMng naborMng;
    private final MeterMng meterMng;
    private final GenPart genPart;
    private final MeterDAO meterDao;
    private final ConfigApp config;
    private final SprParamMng sprParamMng;

    @PersistenceContext
    private EntityManager em;

    public GenChrgProcessMngImpl(NaborMng naborMng,
                                 MeterMng meterMng, GenPart genPart, MeterDAO meterDao,
                                 ConfigApp config, SprParamMng sprParamMng) {
        this.naborMng = naborMng;
        this.meterMng = meterMng;
        this.genPart = genPart;
        this.meterDao = meterDao;
        this.config = config;
        this.sprParamMng = sprParamMng;
    }

    /**
     * Рассчитать начисление
     * Внимание! Расчет идёт по помещению (помещению), но информация группируется по лиц.счету(Kart)
     * так как теоретически может быть одинаковая услуга на разных лиц.счетах, но на одной помещению!
     * ОПИСАНИЕ: https://docs.google.com/document/d/1mtK2KdMX4rGiF2cUeQFVD4HBcZ_F0Z8ucp1VNK8epx0/edit
     *
     * @param reqConf - конфиг запроса
     * @param klskId  - klskId помещения
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRES_NEW,
            rollbackFor = Exception.class)
    public void genChrg(RequestConfigDirect reqConf, long klskId) throws ErrorWhileChrg {
        // заблокировать объект Ko для расчета
        if (!config.getLock().aquireLockId(reqConf.getRqn(), 1, klskId, 60)) {
            throw new ErrorWhileChrg("ОШИБКА БЛОКИРОВКИ klskId=" + klskId);
        }
        try {
            CalcStore calcStore = reqConf.getCalcStore();
            Ko ko = em.getReference(Ko.class, klskId);

            // создать локальное хранилище объемов
            ChrgCountAmountLocal chrgCountAmountLocal = new ChrgCountAmountLocal();
            // параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 - для ТСЖ (пока, может поправить)
            int parVarCntKpr =
                    Utl.nvl(sprParamMng.getN1("VAR_CNT_KPR"), 0D).intValue();
            // параметр учета проживающих для капремонта
            int parCapCalcKprTp =
                    Utl.nvl(sprParamMng.getN1("CAP_CALC_KPR_TP"), 0D).intValue();

            //ChrgCount chrgCount = new ChrgCount();
            // выбранные услуги для формирования
            List<Usl> lstSelUsl = new ArrayList<>();
            if (Utl.in(reqConf.getTp(), 0, 4) && reqConf.getUsl() != null) {
                // начисление по выбранной услуге, начисление по одной услуге, для автоначисления
                lstSelUsl.add(reqConf.getUsl());
            } else if (Utl.in(reqConf.getTp(), 3)) {
                // начисление для распределения по вводу
                // добавить услуги для ограничения формирования только по ним
                lstSelUsl.add(reqConf.getVvod().getUsl());
            }

            // все действующие счетчики объекта и их объемы
            List<SumMeterVol> lstMeterVol = meterDao.getMeterVolByKlskId(ko.getId(),
                    calcStore.getCurDt1(), calcStore.getCurDt2());
            /*System.out.println("Счетчики:");
            for (SumMeterVol t : lstMeterVol) {
                log.trace("t.getMeterId={}, t.getUslId={}, t.getDtTo={}, t.getDtFrom={}, t.getVol={}",
                        t.getMeterId(), t.getUslId(), t.getDtTo(), t.getDtFrom(), t.getVol());
            }*/
            // получить объемы по счетчикам в пропорции на 1 день их работы
            List<UslMeterDateVol> lstDayMeterVol = meterMng.getPartDayMeterVol(lstMeterVol,
                    calcStore);

            Calendar c = Calendar.getInstance();

            // получить действующие, отсортированные услуги по помещению (по всем счетам)
            // перенести данный метод внутрь genVolPart, после того как будет реализованна архитектурная возможность
            // отключать услугу в течении месяца
            List<Nabor> lstNabor = naborMng.getActualNabor(ko, null);

            // очистить информационные строки по льготам
            lstNabor.stream().map(Nabor::getKart).distinct().forEach(t ->
                    t.getChargePrep().removeIf(chargePrep -> chargePrep.getTp().equals(9)));

            // РАСЧЕТ по блокам:

            // 1. Основные услуги
            // цикл по дням месяца
            int part = 1;
            log.trace("Расчет объемов услуг, до учёта экономии ОДН");
            for (c.setTime(calcStore.getCurDt1()); !c.getTime()
                    .after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
                genPart.genVolPart(chrgCountAmountLocal, reqConf, parVarCntKpr,
                        parCapCalcKprTp, ko, lstMeterVol, lstSelUsl, lstDayMeterVol, c.getTime(), part, lstNabor);
            }

            // кроме распределения объемов (там нечего еще считать, нет экономии ОДН
            if (!Utl.in(reqConf.getTp(), 3, 4)) {
                // 2. распределить экономию ОДН по услуге, пропорционально объемам
                log.trace("Распределение экономии ОДН");
                distODNeconomy(chrgCountAmountLocal, ko, lstSelUsl);

                // 3. Зависимые услуги, которые необходимо рассчитать после учета экономии ОДН в основных расчетах
                // цикл по дням месяца (например calcTp=47 - Тепл.энергия для нагрева ХВС или calcTp=19 - Водоотведение)
                part = 2;
                log.trace("Расчет объемов услуг, после учёта экономии ОДН");
                for (c.setTime(calcStore.getCurDt1()); !c.getTime()
                        .after(calcStore.getCurDt2()); c.add(Calendar.DATE, 1)) {
                    genPart.genVolPart(chrgCountAmountLocal, reqConf, parVarCntKpr,
                            parCapCalcKprTp, ko, lstMeterVol, lstSelUsl, lstDayMeterVol, c.getTime(), part, lstNabor);
                }
            }

            // 4. Округлить объемы
            chrgCountAmountLocal.roundVol();
            //chrgCountAmountLocal.printVolAmntChrg();

            if (reqConf.getTp() == 3) {
                // 5. Добавить в объемы по вводу
                reqConf.getChrgCountAmount().append(chrgCountAmountLocal);
            }

            chrgCountAmountLocal.printVolAmnt(null, "После округления");

            if (!Utl.in(reqConf.getTp(), 3, 4)) {
                // 6. Сгруппировать строки начислений для записи в C_CHARGE
                //chrgCountAmountLocal.printVolAmntChrg();
                chrgCountAmountLocal.groupUslVolChrg();

                // 7. Умножить объем на цену (расчет в рублях), сохранить в C_CHARGE, округлить для ГИС ЖКХ
                chrgCountAmountLocal.saveChargeAndRound(ko);

                // 9. Сохранить фактическое наличие счетчика, в случае отсутствия объема, для формирования статистики
                chrgCountAmountLocal.saveFactMeterTp(ko, lstMeterVol, reqConf.getCalcStore().getCurDt2());
            }

            if (reqConf.getTp() == 4) {
                // 10. по операции - начисление по одной услуге, для автоначисления - по нормативу
                // заполнить итоговый объем
                reqConf.getChrgCountAmount().setResultVol(chrgCountAmountLocal.getAmntVolByUsl(reqConf.getUsl()));
            }

        } catch (WrongParam wrongParam) {
            log.error(Utl.getStackTraceString(wrongParam));
            throw new ErrorWhileChrg("Ошибка в использовании параметра");
        } finally {
            // разблокировать помещение
            config.getLock().unlockId(reqConf.getRqn(), 1, klskId);
            //log.info("******* klskId={} разблокирован после расчета", klskId);
        }
    }

    /**
     * Распределить объемы экономии по услугам лиц.счетов, рассчитанных по помещению
     *
     * @param chrgCountAmountLocal - локальное хранилище объемов по помещению
     * @param ko                   - помещение
     * @param lstSelUsl            - список ограничения услуг (например при распределении ОДН)
     */
    private synchronized void distODNeconomy(ChrgCountAmountLocal chrgCountAmountLocal,
                                             Ko ko, List<Usl> lstSelUsl) {
        // получить объемы экономии по всем лиц.счетам помещения
        List<ChargePrep> lstChargePrep = ko.getKart().stream()
                .flatMap(t -> t.getChargePrep().stream())
                .filter(c -> c.getTp().equals(4) && lstSelUsl.size() == 0)
                .filter(c -> c.getKart().getNabor().stream()
                        .anyMatch(n -> n.getUsl().equals(c.getUsl().getUslChild())
                                && n.isValid(true))) // только по действительным услугам ОДН
                .collect(Collectors.toList());

        // распределить экономию
        for (ChargePrep t : lstChargePrep) {
            // РАСПРЕДЕЛИТЬ весь объем экономии по элементам объема в лиц.счете (когда были проживающие)
            List<UslVolKart> lstUslVolKart = chrgCountAmountLocal.getLstUslVolKart().stream()
                    .filter(d -> d.getKart().equals(t.getKart()) && d.getKprNorm().compareTo(BigDecimal.ZERO) != 0 && d.getUsl().equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов лиц.счета
            Utl.distBigDecimalByList(t.getVol(), lstUslVolKart, 5);

            // РАСПРЕДЕЛИТЬ весь объем экономии по элементам объема во вводе (когда были проживающие)
            List<UslVolVvod> lstUslVolVvod = chrgCountAmountLocal.getLstUslVolVvod().stream()
                    .filter(d -> d.getKprNorm().compareTo(BigDecimal.ZERO) != 0 && d.getUsl().equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов ввода
            Utl.distBigDecimalByList(t.getVol(), lstUslVolVvod, 5);

            // РАСПРЕДЕЛИТЬ по датам, детально
            // в т.ч. для услуги calcTp=47 (Тепл.энергия для нагрева ХВС (Кис.)) (когда были проживающие)
            // в т.ч. по услугам х.в. и г.в. (для водоотведения)
            List<UslPriceVolKart> lstUslPriceVolKart = chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                    .filter(d -> d.getKart().equals(t.getKart()) && d.getKprNorm().compareTo(BigDecimal.ZERO) != 0
                            && d.getUsl().equals(t.getUsl()))
                    .collect(Collectors.toList());

            // распределить объем экономии по списку объемов лиц.счета, по датам
            Utl.distBigDecimalByList(t.getVol(), lstUslPriceVolKart, 5);

            // ПО СГРУППИРОВАННЫМ объемам до лиц.счетов, просто снять объем
            chrgCountAmountLocal.getLstUslVolKartGrp().stream()
                    .filter(d -> d.getKart().equals(t.getKart()) && d.getUsl().equals(t.getUsl()))
                    .findFirst().ifPresent(uslVolKartGrp -> uslVolKartGrp.setVol(uslVolKartGrp.getVol().add(t.getVol())));
        }
    }
}