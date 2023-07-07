package com.dic.bill.dto;

import ch.qos.logback.classic.Logger;
import com.dic.bill.dao.ChargeDAO;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.dto.SumMeterVol;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Хранилище объемов по помещению (локальное использование)
 */
@Getter
@Setter
@Slf4j
@Service
@Scope("prototype")
@Transactional
@RequiredArgsConstructor
public class ChrgCountAmountLocal extends ChrgCountAmountBase {

    @PersistenceContext
    private EntityManager em;

    // с полной детализацией по услуге используется выборочными услугами
    private List<UslPriceVolKart> lstUslPriceVolKartDetailed = new ArrayList<>(10);

    // сгруппированное до дат, для подготовки записи результата начисления в C_CHARGE
    // (возможно в будущем, прям отсюда записывать в C_CHARGE - детально)
    private List<UslPriceVolKartDt> lstUslPriceVolKartDt = new ArrayList<>(10);

    // сгруппированное до фактических услуг, для записи в C_CHARGE
    private List<UslVolCharge> lstUslVolCharge = new ArrayList<>(10);

    // признак добавления информации по льготе по капремонту
    boolean isCapPrivAdded = false;

    // признак добавления информации по кол-ву проживающих, для ведомости субсидирования
    Set<String> kprByTp1Added = new HashSet<>();

    private final ChargeDAO chargeDAO;

    /**
     * сгруппировать объемы для распределения по вводам
     *
     * @param u - объект объема
     */
    public void groupUslVol(UslPriceVolKart u) {
        // note ред.22.02.21 - возможны проблемы с добавлением объемов по услуге 54 - отопление гкал со сверх.норм.
        // note пока не стал вносить изменения
        // детализированный объем
        // округленный объем
        BigDecimal vol = u.vol.add(u.volOverSoc);
        // Сгруппировать объемы по базовым параметрам, по лиц.счетам для распределения по вводам
        UslVolKartGrp prevUslVolKartGrp = getLstUslVolKartGrp().stream().filter(t -> t.kart.equals(u.kart)
                && t.usl.equals(u.usl)
        ).findFirst().orElse(null);
        if (prevUslVolKartGrp == null) {
            // добавить новый элемент
            UslVolKartGrp uslVolKartGrp = new UslVolKartGrp();
            uslVolKartGrp.kart = u.kart;
            // жилое помещение (берётся по Основному лиц.счету)
            uslVolKartGrp.isResidental = u.isResidental;
            uslVolKartGrp.usl = u.usl;
            uslVolKartGrp.vol = vol;

            uslVolKartGrp.area = u.area.add(u.areaOverSoc);
            uslVolKartGrp.kprNorm = u.kprNorm;
            if (u.isMeter) {
                // если хоть один раз был в периоде счетчик - поставить отметку
                uslVolKartGrp.isExistMeterCurrPeriod = true;
            }
            if (!u.isEmpty) {
                // если хоть один раз были в периоде зарегистрированы проживающие - поставить отметку
                uslVolKartGrp.isExistPersCurrPeriod = true;
            }
            getLstUslVolKartGrp().add(uslVolKartGrp);
        } else {
            // такой же по ключевым параметрам, добавить данные в найденную строку
            prevUslVolKartGrp.vol = prevUslVolKartGrp.vol.add(vol);
            prevUslVolKartGrp.area = prevUslVolKartGrp.area.add(u.area).add(u.areaOverSoc);
            prevUslVolKartGrp.kprNorm = prevUslVolKartGrp.kprNorm.add(u.kprNorm);
        }

        // если услуга usl.cd="х.в. для гвс", то сохранить для услуг типа Тепл.энергия для нагрева ХВС (Кис.)
        // или х.в., г.в. для водоотведения
        if (u.usl.isBaseWaterCalc() || (u.usl.getCd() != null &&
                u.usl.getCd().equals("х.в. для гвс"))) {
            lstUslPriceVolKartDetailed.add(u);
        }

        // Сгруппировать объемы по лиц.счетам для распределения по вводам
        UslVolKart prevUslVolKart = getLstUslVolKart().stream().filter(t -> t.kart.equals(u.kart)
                && t.usl.equals(u.usl) && t.isMeter == u.isMeter
                && t.isEmpty == u.isEmpty && t.isResidental == u.isResidental
        ).findFirst().orElse(null);
        if (prevUslVolKart == null) {
            // добавить новый элемент
            UslVolKart uslVolKart = new UslVolKart();
            uslVolKart.kart = u.kart;
            uslVolKart.usl = u.usl;
            uslVolKart.isResidental = u.isResidental;
            uslVolKart.isMeter = u.isMeter;
            uslVolKart.isEmpty = u.isEmpty;
            uslVolKart.vol = vol;
            uslVolKart.area = u.area.add(u.areaOverSoc);
            uslVolKart.kprNorm = u.kprNorm;
            getLstUslVolKart().add(uslVolKart);
        } else {
            // такой же по ключевым параметрам, добавить данные в найденную строку
            prevUslVolKart.vol = prevUslVolKart.vol.add(vol);
            prevUslVolKart.area = prevUslVolKart.area.add(u.area).add(u.areaOverSoc);
            prevUslVolKart.kprNorm = prevUslVolKart.kprNorm.add(u.kprNorm);
        }

        // Сгруппировать по вводу
        UslVolVvod prevUslVolVvod = getLstUslVolVvod().stream().filter(t ->
                t.usl.equals(u.usl) && t.isMeter == u.isMeter
                        && t.isEmpty == u.isEmpty && t.isResidental == u.isResidental
        ).findFirst().orElse(null);
        if (prevUslVolVvod == null) {
            // добавить новый элемент
            UslVolVvod uslVolVvod = new UslVolVvod();
            uslVolVvod.isResidental = u.isResidental;
            uslVolVvod.isMeter = u.isMeter;
            uslVolVvod.isEmpty = u.isEmpty;
            uslVolVvod.usl = u.usl;
            uslVolVvod.vol = vol;
            uslVolVvod.area = u.area;
            uslVolVvod.kprNorm = u.kprNorm;
            getLstUslVolVvod().add(uslVolVvod);
        } else {
            // такой же по ключевым параметрам, добавить данные в найденную строку
            prevUslVolVvod.vol = prevUslVolVvod.vol.add(vol);
            prevUslVolVvod.area = prevUslVolVvod.area.add(u.area).add(u.areaOverSoc);
            prevUslVolVvod.kprNorm = prevUslVolVvod.kprNorm.add(u.kprNorm);
        }

        // Сгруппировать до дат, для записи реультата начисления в C_CHARGE
        if (/*vol.compareTo(BigDecimal.ZERO) != 0 && note ред. 05.04.19 Кис. попросили делать пустую строку, даже если нет объема, для статы*/
                u.isForChrg() && (u.price.compareTo(BigDecimal.ZERO) != 0 || !u.isResidental
                        || u.getUsl().getFkCalcTp().equals(34))) { // объем не нулевой и цена не нулевая или услуга Повыш коэфф для Полыс.
            Date prevDt = Utl.addDays(u.dt, -1);
            // искать по лиц.счету, предыдущей дате, основному ключу
            UslPriceVolKartDt prevUslPriceVolKartDt = lstUslPriceVolKartDt.stream().filter(t -> t.kart.equals(u.kart)
                    && t.dtTo.equals(prevDt)
                    && t.usl.equals(u.usl) &&
                    t.org.equals(u.org) && t.isMeter == u.isMeter && t.isResidental == u.isResidental
                    && t.isEmpty == u.isEmpty && t.socStdt.equals(u.socStdt)
                    && t.price.equals(u.price) && t.priceOverSoc.equals(u.priceOverSoc)
                    && t.priceEmpty.equals(u.priceEmpty)).findFirst().orElse(null);
            if (prevUslPriceVolKartDt == null) {
                // добавить новый элемент
                UslPriceVolKartDt uslPriceVolKartDt = new UslPriceVolKartDt();
                uslPriceVolKartDt.kart = u.kart;
                uslPriceVolKartDt.usl = u.usl;
                uslPriceVolKartDt.uslOverSoc = u.uslOverSoc;
                uslPriceVolKartDt.uslEmpt = u.uslEmpt;
                uslPriceVolKartDt.dtFrom = u.dt;
                uslPriceVolKartDt.dtTo = u.dt;
                uslPriceVolKartDt.isEmpty = u.isEmpty;
                uslPriceVolKartDt.isMeter = u.isMeter;
                uslPriceVolKartDt.isResidental = u.isResidental;

                uslPriceVolKartDt.vol = u.vol;
                uslPriceVolKartDt.volOverSoc = u.volOverSoc;

                uslPriceVolKartDt.area = u.area;
                uslPriceVolKartDt.areaOverSoc = u.areaOverSoc;

                uslPriceVolKartDt.kpr = u.kpr;
                uslPriceVolKartDt.kprNorm = u.kprNorm;
                uslPriceVolKartDt.kprOt = u.kprOt;
                uslPriceVolKartDt.kprWr = u.kprWr;

                uslPriceVolKartDt.org = u.org;
                uslPriceVolKartDt.socStdt = u.socStdt;

                uslPriceVolKartDt.price = u.price;
                uslPriceVolKartDt.priceEmpty = u.priceEmpty;
                uslPriceVolKartDt.priceOverSoc = u.priceOverSoc;

                lstUslPriceVolKartDt.add(uslPriceVolKartDt);
            } else {
                // такой же по ключевым параметрам, добавить данные в найденный период
                prevUslPriceVolKartDt.area = prevUslPriceVolKartDt.area.add(u.area);
                prevUslPriceVolKartDt.areaOverSoc = prevUslPriceVolKartDt.areaOverSoc.add(u.areaOverSoc);

                prevUslPriceVolKartDt.vol = prevUslPriceVolKartDt.vol.add(u.vol);
                prevUslPriceVolKartDt.volOverSoc = prevUslPriceVolKartDt.volOverSoc.add(u.volOverSoc);

                prevUslPriceVolKartDt.kpr = prevUslPriceVolKartDt.kpr.add(u.kpr);
                prevUslPriceVolKartDt.kprNorm = prevUslPriceVolKartDt.kprNorm.add(u.kprNorm);
                prevUslPriceVolKartDt.kprWr = prevUslPriceVolKartDt.kprWr.add(u.kprWr);
                prevUslPriceVolKartDt.kprOt = prevUslPriceVolKartDt.kprOt.add(u.kprOt);

                // продлить дату окончания
                prevUslPriceVolKartDt.dtTo = u.dt;
            }
        }
    }

    /**
     * Округлить объемы по всем услугам
     */
    public void roundVol() {
        List<Usl> lstUsl = getLstUslVolKartGrp().stream().map(t -> t.usl).distinct().collect(Collectors.toList());
        for (Usl usl : lstUsl) {
            BigDecimal summSample = roundByLst(getLstUslVolKartGrp(), usl, null);
            roundByLst(getLstUslVolVvod(), usl, summSample);
            roundByLst2(getLstUslPriceVolKartDt(), usl, summSample);
            //log.trace("usl={}", usl.getId());
        }

/* какой бред!
        getLstUslPriceVolKartDt().stream().forEach(t-> log.trace("usl2={}", t.usl.getId()));
        // ред.22.02.21 - округлить услуги не вошедшие в список округляемых по вводу
        List<Usl> lstUslOther = getLstUslPriceVolKartDt().stream()
                .filter(t -> !lstUsl.contains(t.usl))
                .map(t -> t.usl).distinct().collect(Collectors.toList());
        for (Usl usl : lstUslOther) {
            roundByLst2(getLstUslPriceVolKartDt(), usl);
        }
*/
    }

    /**
     * Получить начисленный объем по помещению, услуге
     */
    public BigDecimal getAmntVolByUsl(Usl usl) {
        return getLstUslVolKartGrp().stream()
                .filter(t -> t.getUsl().equals(usl))
                .map(UslVol::getVol)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * Округлить по коллекциям
     *
     * @param lstSrc     - коллекция
     * @param usl        - услуга
     * @param summSample - сумма для сравнения
     * @return - сумма для сравнения в будущих округлениях
     */
    private BigDecimal roundByLst(List<? extends UslVol> lstSrc, Usl usl, BigDecimal summSample) {
        List<UslVol> lst = lstSrc.stream()
                .filter(t -> t.usl.equals(usl))
                .collect(Collectors.toList());

        int round;
        // note Для отображения округлённого объема в карточках так же используется поле USL.CHRG_ROUND!
        if (usl.isCalcByArea()) {
            // до 2 знаков - услуги, рассчитываемые по площади
            round = 2;
        } else {
            // остальные услуги до 5 знаков
            round = 5;
        }
        if (summSample == null) {
            summSample = lst.stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add)
                    .setScale(round, BigDecimal.ROUND_HALF_UP);
        }
        //log.info("$$$$$$$ summSample={}", summSample);
        lst.forEach(t -> t.vol = t.vol.setScale(round, BigDecimal.ROUND_HALF_UP));
        BigDecimal sumVol2 = lst.stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add);
        //log.info("$$$$$$$ sumVol2={}", sumVol2);
        BigDecimal diff = summSample.subtract(sumVol2);
        //log.info("$$$$$$$ diff={}", diff);
        if (diff.compareTo(BigDecimal.ZERO) != 0) {
            lstSrc.stream()
                    .filter(t -> t.usl.equals(usl))
                    .reduce((first, second) -> second) // найти последний элемент
                    .ifPresent(t -> t.vol = t.vol.add(diff));
        }
        return summSample;
    }

    private BigDecimal roundByLst2(List<UslPriceVolKartDt> lstSrc, Usl usl, BigDecimal summSample) {
        List<UslPriceVolKartDt> lst = lstSrc.stream()
                .filter(t -> t.usl.equals(usl))
                .collect(Collectors.toList());

        int round;
        // note Для отображения округлённого объема в карточках так же используется поле USL.CHRG_ROUND!
        if (usl.isCalcByArea()) {
            // до 2 знаков - услуги, рассчитываемые по площади
            round = 2;
        } else {
            // остальные услуги до 5 знаков
            round = 5;
        }
        if (summSample == null) {
            summSample = lst.stream().map(t -> t.vol.add(t.volOverSoc)).reduce(BigDecimal.ZERO, BigDecimal::add)
                    .setScale(round, BigDecimal.ROUND_HALF_UP);
        }
        lst.forEach(t -> {
            t.vol = t.vol.setScale(round, BigDecimal.ROUND_HALF_UP);
            t.volOverSoc = t.volOverSoc.setScale(round, BigDecimal.ROUND_HALF_UP);
        });

        BigDecimal sumVol2 = lst.stream().map(t -> t.vol.add(t.volOverSoc)).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal diff = summSample.subtract(sumVol2);
        if (diff.compareTo(BigDecimal.ZERO) != 0) {
            lstSrc.stream()
                    .filter(t -> t.usl.equals(usl))
                    .reduce((first, second) -> second) // найти последний элемент
                    .ifPresent(t -> t.vol = t.vol.add(diff));
        }
        return summSample;
    }

/*
    private void roundByLst2(List<UslPriceVolKartDt> lstSrc, Usl usl) {
        List<UslPriceVolKartDt> lst = lstSrc.stream()
                .filter(t -> t.usl.equals(usl))
                .collect(Collectors.toList());

        int round;
        if (usl.isCalcByArea()) {
            // до 2 знаков - услуги, рассчитываемые по площади
            round = 2;
        } else {
            // остальные услуги до 5 знаков
            round = 5;
        }
        BigDecimal summSample = lst.stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(round, BigDecimal.ROUND_HALF_UP);
        BigDecimal summSampleOverSoc = lst.stream().map(t -> t.volOverSoc).reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(round, BigDecimal.ROUND_HALF_UP);
        lst.forEach(t -> t.vol = t.vol.setScale(round, BigDecimal.ROUND_HALF_UP));
        lst.forEach(t -> t.volOverSoc = t.volOverSoc.setScale(round, BigDecimal.ROUND_HALF_UP));
        BigDecimal sumVol = lst.stream().map(t -> t.vol).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal diff = summSample.subtract(sumVol);
        sumVol = lst.stream().map(t -> t.volOverSoc).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal diffOverSoc = summSampleOverSoc.subtract(sumVol);
        if (diff.compareTo(BigDecimal.ZERO) != 0 || diffOverSoc.compareTo(BigDecimal.ZERO) != 0) {
            lstSrc.stream()
                    .filter(t -> t.usl.equals(usl))
                    .reduce((first, second) -> second) // найти последний элемент
                    .ifPresent(t ->
                            {
                                t.vol = t.vol.add(diff);
                                t.volOverSoc = t.volOverSoc.add(diffOverSoc);
                            }
                    );
        }
    }
*/

    /**
     * Распечатать объемы по лиц.счетам
     *
     * @param uslId - код услуги, если не заполнено, то все
     * @param msg   - сообщение
     */
    public void printVolAmnt(String uslId, String msg) {
        Logger root = (Logger) LoggerFactory.getLogger("com.ric");
        if (root.isTraceEnabled()) {
            log.trace("");
            log.trace("****** ПРОВЕРКА объемов {}, по UslPriceVolKartDt:", msg);
            // отсортировать по lsk, usl, dtFrom
            List<UslPriceVolKartDt> lst =
                    getLstUslPriceVolKartDt().stream()
                            .sorted(Comparator.comparing((UslPriceVolKartDt o1) -> o1.getKart().getLsk())
                                    .thenComparing((UslPriceVolKartDt o1) -> o1.getUsl().getId())
                                    .thenComparing((UslPriceVolKartDt o1) -> o1.dtFrom)
                            )
                            .collect(Collectors.toList());
            for (UslPriceVolKartDt t : lst) {
                if (uslId == null || t.usl.getId().equals(uslId)) {
                    log.trace("dt={}-{}, lsk={}, usl={}, uslOverSoc={}, uslEmpt={}, ar={}, arOv={}, empt={}, met={}, res={}, " +
                                    "org={}, prc={}, prcE={}, prcO={}, std={} " +
                                    "kprNorm={}, kprO={}, kprW={}, kprM={}, vol={}, volOvSc={}",
                            Utl.getStrFromDate(t.dtFrom), Utl.getStrFromDate(t.dtTo),
                            t.kart.getLsk(), t.usl.getId(), t.uslOverSoc.getId(), t.uslEmpt.getId(),
                            t.area.setScale(4, BigDecimal.ROUND_HALF_UP).stripTrailingZeros(),
                            t.areaOverSoc.setScale(4, BigDecimal.ROUND_HALF_UP).stripTrailingZeros(),
                            t.isEmpty ? "T" : "F",
                            t.isMeter ? "T" : "F",
                            t.isResidental ? "T" : "F",
                            t.org.getId(), t.price, t.priceEmpty, t.priceOverSoc, t.socStdt,
                            t.kprNorm.setScale(4, BigDecimal.ROUND_HALF_UP).stripTrailingZeros(),
                            t.kprOt.setScale(4, BigDecimal.ROUND_HALF_UP).stripTrailingZeros(),
                            t.kprWr.setScale(4, BigDecimal.ROUND_HALF_UP).stripTrailingZeros(),
                            t.kprNorm.setScale(4, BigDecimal.ROUND_HALF_UP).stripTrailingZeros(),
                            t.vol.setScale(8, BigDecimal.ROUND_HALF_UP).stripTrailingZeros(),
                            t.volOverSoc.setScale(8, BigDecimal.ROUND_HALF_UP).stripTrailingZeros()
                    );
                }
            }
            log.trace("");
            log.trace("****** ПРОВЕРКА объемов {}, по UslPriceVolKartGrp:", msg);
            // отсортировать по lsk, usl, dtFrom
            for (UslVolKartGrp t : getLstUslVolKartGrp()) {
                if (uslId == null || t.usl.getId().equals(uslId)) {
                    log.trace("lsk={}, usl={}, isResid={}, isMeter={}, isPers={}, ar={}, vol={}, kprNorm={}",
                            t.kart.getLsk(), t.usl.getId(), t.isResidental,
                            t.isExistMeterCurrPeriod, t.isExistPersCurrPeriod, t.area, t.vol, t.kprNorm);
                }
            }
            log.trace("");
            log.trace("****** ПРОВЕРКА объемов {}, по UslPriceVolVvod:", msg);
            // отсортировать по lsk, usl, dtFrom
            for (UslVolVvod t : getLstUslVolVvod()) {
                if (uslId == null || t.usl.getId().equals(uslId)) {
                    log.trace("usl={}, isResid={}, isMeter={}, isEmpt={}, ar={}, vol={}, kprNorm={}",
                            t.usl.getId(), t.isResidental,
                            t.isMeter, t.isEmpty, t.area, t.vol, t.kprNorm);
                }
            }

        }
    }

    /**
     * Распечатать объемы по лиц.счетам для начисления
     */

    public void printVolAmntChrg() {
        Logger root = (Logger) LoggerFactory.getLogger("com.ric");
        if (root.isTraceEnabled()) {
            log.trace("");
            log.trace("****** ПРОВЕРКА объема UslPriceVolKartDt, для сохранения в C_CHARGE:");
            for (UslPriceVolKartDt u : getLstUslPriceVolKartDt()) {
                log.trace("lsk={}, usl={}, vol={}, volOverSoc={} *******",
                        u.kart.getLsk(), u.usl.getId(), u.vol, u.volOverSoc);
            }
        }
    }

    /**
     * сгруппировать и сохранить начисление
     * примечание: так как пока не реализовано хранение организации в C_CHARGE, не группировать по org. ред. 30.01.19
     */
    public void groupUslVolChrg() {
        for (UslPriceVolKartDt u : getLstUslPriceVolKartDt()) {
            Usl uslFact;
            Org orgFact;
            BigDecimal priceFact;
            //if (u.vol.compareTo(BigDecimal.ZERO) != 0) { //note  ред. 05.04.19 закомментировал - Кис. попросили делать пустую строку, даже если нет объема, для статы
            // прочие услуги
            if (!u.isEmpty) {
                // есть проживающие
                uslFact = u.usl;
                orgFact = u.org;
                priceFact = u.price;
            } else {
                // нет проживающих
                uslFact = u.uslEmpt;
                orgFact = u.org;
                priceFact = u.priceEmpty;
            }
            addUslVolChrg(u, uslFact, orgFact, u.vol, u.area, priceFact, false);
            //}

            // свыше соц.нормы
            if (u.volOverSoc.compareTo(BigDecimal.ZERO) != 0) {
                addUslVolChrg(u, u.uslOverSoc, u.org,
                        u.volOverSoc, u.areaOverSoc, u.priceOverSoc, true);
            }
        }
    }

    /**
     * добавить строку для записи в C_CHARGE, с группировкой
     *
     * @param u         запись начисления
     * @param uslFact   фактическая услуга
     * @param vol       объем
     * @param area      площадь
     * @param isOverSoc
     */
    private void addUslVolChrg(UslPriceVolKartDt u, Usl uslFact, Org orgFact,
                               BigDecimal vol, BigDecimal area, BigDecimal price, boolean isOverSoc) {
        UslVolCharge prev = getLstUslVolCharge().stream()
                .filter(t -> t.kart.equals(u.kart) && t.usl.equals(uslFact) && t.org.equals(orgFact)
                        && t.price.equals(price)
                        && t.isMeter == u.isMeter)
                .findFirst().orElse(null);
        if (prev != null) {
            // найдена запись с данным ключом
            prev.vol = prev.vol.add(vol);
            prev.area = prev.area.add(area);
            if (!isOverSoc) {
                prev.setKpr(prev.getKpr().add(u.getKpr()));
                prev.setKprNorm(prev.getKprNorm().add(u.kprNorm));
                prev.kprWr = prev.kprWr.add(u.kprWr);
                prev.kprOt = prev.kprOt.add(u.kprOt);
            }
        } else {
            // не найдена запись, создать новую
            UslVolCharge uslVolCharge = new UslVolCharge();
            uslVolCharge.kart = u.kart;
            uslVolCharge.usl = uslFact;
            uslVolCharge.org = orgFact;
            uslVolCharge.isMeter = u.isMeter;
            uslVolCharge.vol = vol;
            uslVolCharge.price = price;
            uslVolCharge.area = area;
            if (!isOverSoc) {
                uslVolCharge.setKpr(u.getKpr());
                uslVolCharge.setKprNorm(u.kprNorm);
                uslVolCharge.kprWr = u.kprWr;
                uslVolCharge.kprOt = u.kprOt;
            }
            getLstUslVolCharge().add(uslVolCharge);
        }
/*
        log.trace("Добавлено в lstUslVolCharge: lsk={}, usl={}, met={}, vol={}, prc={}, " +
                        "ar={}, kpr={}, kprWr={}, kprOt={}", u.kart.getLsk(), uslFact.getId(),
                u.isMeter, vol, price, area, u.kpr, u.kprWr, u.kprOt);
*/
    }

    /**
     * Сохранить и округлить начисление в C_CHARGE
     *
     * @param ko          фин.лиц.сч.
     * @param mapUslRound услуги для округления в ГИС ЖКХ
     * @return начисление по лиц.счетам в данном фин.лиц.сч.
     */
    public List<LskChargeUsl> saveChargeAndRound(Ko ko, boolean isSaveResult, Map<String, Set<String>> mapUslRound, String period) throws ErrorWhileChrg {
        Map<String, List<LskChargeUsl>> chargeByLsk = new HashMap<>();
        for (Kart kart : ko.getKart()) {
            chargeByLsk.putIfAbsent(kart.getLsk(), new ArrayList<>());
        }
        chargeDAO.deleteAllByKartLskInAndTypeIn(ko.getKart().stream().map(Kart::getLsk).collect(Collectors.toList()), List.of(0,1));
        log.trace("Сохранено в C_CHARGE:");
        int i = 0; // № п.п.
        for (UslVolCharge uslVolCharge : getLstUslVolCharge()) {
            Optional<LskChargeUsl> lskChargeUsl = Optional.empty();
            if (uslVolCharge.kart.getKartExt() != null && uslVolCharge.kart.getKartExt().size() > 0) {
                // внешние лиц.счета, получить сумму начисления из внешнего источника
                for (KartExt kartExt : uslVolCharge.kart.getKartExt()) {
                    if (kartExt.isActual()) {
                        BigDecimal area = uslVolCharge.area.setScale(2, BigDecimal.ROUND_HALF_UP);
                        BigDecimal summa = kartExt.getChrg();

                        // тип 1
                        lskChargeUsl = addCharge(i, uslVolCharge, area, summa, isSaveResult, period);
                        // тип 0 убрал нафиг, если что, потом добавим эту строчку, простой копией из type=1 ред.07.04.2021
                        //addCharge(i, 0, u, area, summa);

                        i++;
                        log.trace("lsk={}, usl={}, org={}, testOpl={}, opl={}, testCena={}, isSch={}, summa={}",
                                uslVolCharge.kart.getLsk(), uslVolCharge.usl.getId(), uslVolCharge.org.getId(),
                                uslVolCharge.vol, area, uslVolCharge.price, uslVolCharge.isMeter, summa);
                        break;
                    }
                }
            } else if (uslVolCharge.getUsl().getFkCalcTp() == null || uslVolCharge.getUsl().getFkCalcTp() != null
                    && !uslVolCharge.getUsl().getFkCalcTp().equals(34)) {
                BigDecimal area = uslVolCharge.area.setScale(2, BigDecimal.ROUND_HALF_UP);
                BigDecimal summa = BigDecimal.ZERO;
                if (!uslVolCharge.getUsl().getIsHideChrg()) {
                    summa = uslVolCharge.vol.multiply(uslVolCharge.price).setScale(2, BigDecimal.ROUND_HALF_UP);
                }
                // тип 1
                lskChargeUsl = addCharge(i, uslVolCharge, area, summa, isSaveResult, period);
                // тип 0 убрал нафиг, если что, потом добавим эту строчку, простой копией из type=1 ред.07.04.2021
                //addCharge(i, 0, u, area, summa);

                i++;
                log.trace("lsk={}, usl={}, org={}, testOpl={}, opl={}, testCena={}, isSch={}, summa={}",
                        uslVolCharge.kart.getLsk(), uslVolCharge.usl.getId(), uslVolCharge.org.getId(),
                        uslVolCharge.vol, area, uslVolCharge.price, uslVolCharge.isMeter, summa);
            }
            lskChargeUsl.ifPresent(t -> chargeByLsk.get(t.lsk).add(t));
        }

        // по услугам, базирующимся на сумме родительской услуги (Повыш. коэфф для Полыс.)
        for (UslVolCharge u : getLstUslVolCharge()) {
            Optional<LskChargeUsl> lskChargeUsl = Optional.empty();
            if (u.getUsl().getFkCalcTp() != null && u.getUsl().getFkCalcTp().equals(34)) {
                BigDecimal area = u.area.setScale(2, BigDecimal.ROUND_HALF_UP);
                BigDecimal parentUslSumma = ko.getKart().stream()
                        .flatMap(t -> t.getCharge().stream())
                        .filter(t -> t.getType().equals(1) && !t.getIsSch() &&
                                // захардкодил условие, так как только для Полыс.
                                (u.getUsl().getId().equals("092") && Utl.in(t.getUsl().getId(), "011", "057")
                                        || u.getUsl().getId().equals("094")
                                        && Utl.in(t.getUsl().getId(), "015", "058", "109", "110"))
                        )
                        .map(Charge::getSumma)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                BigDecimal summa = BigDecimal.ZERO;
                if (!u.getUsl().getIsHideChrg()) {
                    summa = parentUslSumma.multiply(u.vol).setScale(2, BigDecimal.ROUND_HALF_UP);
                }
                // тип 1
                lskChargeUsl = addCharge(i, u, area, summa, isSaveResult, period);
                // тип 0 убрал нафиг, если что, потом добавим эту строчку, простой копией из type=1 ред.07.04.2021
                //addCharge(i, 0, u, area,
                //        summa);
                i++;
                log.trace("lsk={}, usl={}, testOpl={}, opl={}, testCena={}, isSch={}, summa={}",
                        u.kart.getLsk(), u.usl.getId(), u.vol, area, u.price, u.isMeter, summa);
            }
            lskChargeUsl.ifPresent(t -> chargeByLsk.get(t.lsk).add(t));
        }

        // округлить для ГИС ЖКХ
        roundForGis(ko, chargeByLsk, mapUslRound);

        return chargeByLsk.entrySet().stream().flatMap(t -> t.getValue().stream()).collect(Collectors.toList());
    }

    /*
     * @param chargeByLsk начисление
     * @param mapUslRound услуги для округления в ГИС ЖКХ
     */
    private void roundForGis(Ko ko, Map<String, List<LskChargeUsl>> chargeByLsk, Map<String, Set<String>> mapUslRound) throws ErrorWhileChrg {
        // округлить для ГИС ЖКХ
        //log.trace("Округление для ГИС ЖКХ:");
        for (Kart kart : ko.getKart()) {
            // по услугам:
            // цена
            Map<String, BigDecimal> mapPrice = new HashMap<>();

            // итоговая сумма
            BigDecimal summAmnt = BigDecimal.ZERO;
            // итоговая цена
            BigDecimal priceAmnt = BigDecimal.ZERO;

            LskChargeUsl firstCharge = null;

            // сохранить все цены, суммы и объемы по услугам
            // по услугам, подлежащим округлению (находящимся в справочнике SCOTT.USL_ROUND)
            // соответствующего REU

            for (LskChargeUsl lskChargeUsl : chargeByLsk.get(kart.getLsk()).stream()
                    .filter(t -> mapUslRound.getOrDefault(kart.getUk().getReu(), Set.of()).contains(t.uslId))
                    .sorted(Comparator.comparing(LskChargeUsl::getUslId)) // сортировать по коду услуги
                    .collect(Collectors.toList())
            ) {
                // цена
                if (mapPrice.get(lskChargeUsl.getUslId()) == null) {
                    mapPrice.put(lskChargeUsl.getUslId(), lskChargeUsl.getPrice());
                    log.trace("usl={}, price={}", lskChargeUsl.getUslId(), lskChargeUsl.getPrice());
                    priceAmnt = priceAmnt.add(lskChargeUsl.getPrice());
                }

                // сумма
                log.trace("lsk={}, summa={}, priceAmnt={}", kart.getLsk(), lskChargeUsl.getSumma(), priceAmnt);
                summAmnt = summAmnt.add(lskChargeUsl.getSumma());
                if (firstCharge == null)
                    firstCharge = lskChargeUsl;

            }
            // округлить на первую услугу по порядку кода USL
            if (firstCharge != null) {
                BigDecimal summCheck = Utl.nvl(firstCharge.getArea(), BigDecimal.ZERO).multiply(priceAmnt).setScale(2, RoundingMode.HALF_UP);
                BigDecimal diff = summCheck.subtract(summAmnt);
                log.trace("Итого сумма ={}, рассчит={}, общ.площадь:{}", summAmnt, summCheck, firstCharge.getArea());
                if (diff.abs().compareTo(new BigDecimal("0.05")) < 0) {
                    log.trace("Применено округление для ГИС ЖКХ, по lsk={}, usl={}, diff={}",
                            kart.getLsk(), firstCharge.getUslId(), diff);
                    firstCharge.setSumma(firstCharge.getSumma().add(diff));
                    if (firstCharge.getCharge() != null) {
                        firstCharge.getCharge().setSumma(firstCharge.getSumma());
                    } else {
                        log.error("НЕТ ЗАПИСИ CHARGE!!!");
                    }
                } else {
                    throw new ErrorWhileChrg("ОШИБКА! Округление для ГИС ЖКХ превысило 0.05 по lsk=" + kart.getLsk());
                }
            }
            // присвоить null, чтобы в коллекции не было ссылки на Entity Charge (освободить память)
            chargeByLsk.get(kart.getLsk()).forEach(t->t.setCharge(null));
        }
    }

    /**
     * Сохранить фактическое наличие счетчика, в случае отсутствия объема, для формирования статистики
     *
     * @param ko          - объект Ko
     * @param lstMeterVol - список актуальных счетчиков и их объемы
     * @param lastDt      - последняя дата месяца
     */
    public void saveFactMeterTp(Ko ko, List<SumMeterVol> lstMeterVol, Date lastDt) {

        boolean existColdWater = false;
        boolean existHotWater = false;
        for (SumMeterVol e : lstMeterVol) {
            if (Utl.between(lastDt, e.getDtFrom(), e.getDtTo())) {
                // на последнюю дату - работающий счетчик
                if (e.getUslId().equals("011")) {
                    existColdWater = true;
                } else if (e.getUslId().equals("015")) {
                    existHotWater = true;
                }
            }
        }
        // по умолачнию - нет счетчиков
        int meterFactTp = 0;
        if (existColdWater && existHotWater) {
            meterFactTp = 1;
        } else if (existColdWater) {
            meterFactTp = 2;
        } else if (existHotWater) {
            meterFactTp = 3;
        }
        int finalMeterFactTp = meterFactTp;
        ko.getKart().forEach(t -> t.setFactMeterTp(finalMeterFactTp));
    }


    /**
     * Добавить строку начисления
     *
     * @param npp   - № п.п.
     * @param u     - объем
     * @param area  - площадь
     * @param summa - сумма
     */
    private Optional<LskChargeUsl> addCharge(int npp, UslVolCharge u, BigDecimal area, BigDecimal summa, boolean isSaveResult, String period) {
        Optional<LskChargeUsl> lskChargeUsl = Optional.empty();
        if (summa.compareTo(BigDecimal.ZERO) != 0) {
            lskChargeUsl = Optional.of(LskChargeUsl.builder()
                    .klskId(u.getKart().getKoKw().getId())
                    .lsk(u.getKart().getLsk())
                    .uslId(u.getUsl().getId())
                    .orgId(u.getOrg().getId())
                    .vol(u.getVol())
                    .summa(summa)
                    .price(u.price)
                    .area(area)
                    .mg(period)
                    .build());
        }
        if (isSaveResult) {
            Charge charge = new Charge();
            charge.setNpp(npp);
            charge.setType(1);
            charge.setUsl(u.usl);
            charge.setOrg(u.org);
            charge.setKart(u.kart);
            charge.setTestOpl(u.vol);
            charge.setOpl(area);
            charge.setTestCena(u.price);
            charge.setKpr(u.getKpr().setScale(5, BigDecimal.ROUND_HALF_UP));
            charge.setKprz(u.kprWr.setScale(5, BigDecimal.ROUND_HALF_UP));
            charge.setKpro(u.kprOt.setScale(5, BigDecimal.ROUND_HALF_UP));
            charge.setKpr2(u.getKprNorm().setScale(5, BigDecimal.ROUND_HALF_UP));
            charge.setIsSch(u.isMeter);
            charge.setSumma(summa);
            u.kart.getCharge().add(charge);
            lskChargeUsl.ifPresent(t -> t.setCharge(charge));
        }
        return lskChargeUsl;
    }
}
