package com.dic.app.service.impl;

import com.dic.app.enums.BaseForDistPays;
import com.dic.app.service.ConfigApp;
import com.dic.app.service.DistPayMng;
import com.dic.app.service.ProcessMng;
import com.dic.app.service.ReferenceMng;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dao.SprProcPayDAO;
import com.dic.bill.dto.Amount;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.dto.UslOrg;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.mm.SprParamMng;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileDistPay;
import com.ric.cmn.excp.WrongParam;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;

import static com.dic.app.service.impl.enums.ProcessTypes.CHARGE_0;

/**
 * Сервис распределения оплаты
 *
 * @version 1.03
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DistPayMngImpl implements DistPayMng {

    private final ProcessMng processMng;
    private final SaldoMng saldoMng;
    private final ConfigApp configApp;
    private final SprProcPayDAO sprProcPayDAO;
    private final SaldoUslDAO saldoUslDAO;
    private final ReferenceMng referenceMng;
    private final SprParamMng sprParamMng;
    private final DistPayWithRestriction distWithRestriction;
    private final DistPayHelper distPayHelper;
    private final EntityManager em;

    /**
     * Распределить платеж (запись в C_KWTP_MG)
     *
     * @param kwtpMgId - ID записи C_KWTP_MG
     * @param lsk      - лиц.счет
     * @param strSumma - сумма оплаты долга
     * @param strPenya - сумма оплаты пени
     * @param dopl     - период оплаты
     * @param nink     - № инкассации
     * @param nkom     - № компьютера
     * @param oper     - код операции
     * @param strDtek  - дата платежа
     * @param strDtInk - дата инкассации
     * @param isTest   - тестирование? (не будет вызвано начисление)
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class)
    public void distKwtpMg(int kwtpMgId,
                           String lsk,
                           String strSumma,
                           String strPenya,
                           String strDebt,
                           String dopl,
                           int nink,
                           String nkom,
                           String oper,
                           String strDtek,
                           String strDtInk,
                           boolean isTest) throws ErrorWhileDistPay {

        try {
            Amount amount = buildAmount(kwtpMgId,
                    !isTest,
                    lsk,
                    strSumma,
                    strPenya,
                    strDebt,
                    dopl,
                    nink,
                    nkom,
                    oper,
                    strDtek,
                    strDtInk);
            if (amount.getDistTp() == 1) {
                // Кис.
                Org uk = amount.getKart().getUk();
                // общий способ распределения
                if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                    distPayHelper.saveKwtpDayLog(amount, "1.0 Сумма оплаты долга > 0");

                    if (amount.getSumma().compareTo(amount.getAmntDebtDopl()) == 0) {
                        distPayHelper.saveKwtpDayLog(amount, "2.0 Сумма оплаты = долг за период");
                        distPayHelper.saveKwtpDayLog(amount, "2.1 Распределить по вх.деб.+кред. по списку закрытых орг, " +
                                "c ограничением по исх.сал.");
                        distWithRestriction.distributeMoney(
                                new DistribParam(amount, BaseForDistPays.IN_SAL_0, true, true, true, true, true, true, false, null, true, null, false));

                        distPayHelper.saveKwtpDayLog(amount, "2.2 Распределить по вх.деб.+кред. остальные услуги, кроме " +
                                "списка закрытых орг. " +
                                "без ограничения по исх.сальдо");
                        distWithRestriction.distributeMoney(
                                new DistribParam(amount, BaseForDistPays.IN_SAL_0, false, null, null, null, null, false, true, null, true, null, false));
                    } else if (amount.getSumma().compareTo(amount.getAmntDebtDopl()) > 0) {
                        distPayHelper.saveKwtpDayLog(amount, "3.0 Сумма оплаты > долг за период (переплата)");
                        boolean flag = false;
                        if (uk.getDistPayTp().equals(0)) {
                            flag = true;
                            distPayHelper.saveKwtpDayLog(amount, "3.1.1 Тип распределения - общий");
                        } else if (amount.getAmntInSal().compareTo(amount.getAmntChrgPrevPeriod()) > 0) {
                            flag = true;
                            distPayHelper.saveKwtpDayLog(amount, "3.1.1 Тип распределения - для УК 14,15 при вх.деб.+вх.кред. " +
                                    "> долг.1 мес.");
                        }
                        if (flag) {
                            distPayHelper.saveKwtpDayLog(amount, "3.1.2 Распределить по вх.деб.+кред. по списку закрытых орг, " +
                                    "c ограничением по исх.сал.");
                            distWithRestriction.distributeMoney(
                                    new DistribParam(amount, BaseForDistPays.IN_SAL_0, true, true, true, true, true, true, false, null, true, null, false));
                            if (amount.isExistSumma()) {
                                distPayHelper.saveKwtpDayLog(amount, "3.1.3 Распределить по начислению предыдущего периода={}, " +
                                                "без ограничения по исх.сал.",
                                        configApp.getPeriodBack());
                                distWithRestriction.distributeMoney(
                                        new DistribParam(amount, BaseForDistPays.BACK_PERIOD_CHARGE_3, false, null, null, null, null, false, false, null, true, null, false));
                            }
                            if (amount.isExistSumma()) {
                                distPayHelper.saveKwtpDayLog(amount, "3.1.4 Распределить по начислению текущего периода={}, " +
                                                "без ограничения по исх.сал.",
                                        configApp.getPeriod());
                                distWithRestriction.distributeMoney(
                                        new DistribParam(amount, BaseForDistPays.CURR_PERIOD_CHARGE_5, false, null, null, null, null, false, false, null, true, null, false));
                            }
                            if (amount.isExistSumma()) {
                                distPayHelper.saveKwtpDayLog(amount, "3.1.5 Распределить по вх.деб.+кред. по всем орг, " +
                                        "без ограничения по исх.сал.");
                                distWithRestriction.distributeMoney(
                                        new DistribParam(amount, BaseForDistPays.IN_SAL_0, false, null, null, null, null, false, false, null, true, null, false));
                            }
                        } else {
                            distPayHelper.saveKwtpDayLog(amount, "3.2.1 Тип распределения - для УК 14,15 при вх.деб.+вх.кред. " +
                                    "<= долг.1 мес.");
                            distPayHelper.saveKwtpDayLog(amount, "3.2.2 Распределить оплату на услугу 003");
                            distExclusivelyBySingleUslIdUk(amount, "003", true);
                            if (amount.isExistSumma()) {
                                distPayHelper.saveKwtpDayLog(amount, "3.2.3 Остаток распределить на услугу usl=003 и УК");
                                distExclusivelyBySingleUslIdUk(amount, "003", true);
                            }
/*
                        distPayHelper.saveKwtpDayLog(amount, "3.2.2 Распределить оплату по вх.деб.+кред. без услуги 003,
                        c ограничением по исх.сал.");
                        distributeMoney.distributeMoney(amount, 0, true, true,
                                true, true,
                                true, false,
                                false, Collections.singletonList("003"), true);
*/
                        }
                    } else {
                        distPayHelper.saveKwtpDayLog(amount, "4.0 Сумма оплаты < долг за период (недоплата)");
                        final BigDecimal rangeBegin = new BigDecimal("0.01");
                        final BigDecimal rangeEnd = new BigDecimal("100");
                        boolean flag = false;
                        if (uk.getDistPayTp() == null) {
                            throw new ErrorWhileDistPay("ОШИБКА! Не установлен тип распределения оплаты " +
                                    "по организации id="
                                    + uk.getId());
                        } else if (uk.getDistPayTp().equals(0)) {
                            flag = true;
                            distPayHelper.saveKwtpDayLog(amount, "4.1.1 Тип распределения - общий");
                        } else if (amount.getAmntDebtDopl().subtract(amount.getSumma()).compareTo(rangeEnd) > 0) {
                            flag = true;
                            distPayHelper.saveKwtpDayLog(amount, "4.1.1 Тип распределения - для УК 14,15 при сумме недоплаты " +
                                    "> 100");
                        }
                        if (flag) {
                            distPayHelper.saveKwtpDayLog(amount, "4.1.2 Распределить по вх.деб.+кред. по списку закрытых орг, " +
                                    "c ограничением по исх.сал.");
                            distWithRestriction.distributeMoney(
                                    new DistribParam(amount, BaseForDistPays.IN_SAL_0, true, true, true, true, true, true, false, null, true, null, false));
                            if (amount.isExistSumma()) {
                                distPayHelper.saveKwtpDayLog(amount, "4.1.3 Распределить по вх.деб.+кред. остальные услуги, " +
                                        "кроме списка закрытых орг. " +
                                        "без ограничения по исх.сальдо");
                                distWithRestriction.distributeMoney(
                                        new DistribParam(amount, BaseForDistPays.IN_SAL_0, false, null, null, null, null, false, true, null, true, null, false));
                            }
                            if (amount.isExistSumma()) {
                                distPayHelper.saveKwtpDayLog(amount, "4.1.4 Распределить по начислению текущего периода=(), " +
                                                "без ограничения по исх.сал.",
                                        configApp.getPeriod());
                                distWithRestriction.distributeMoney(
                                        new DistribParam(amount, BaseForDistPays.CURR_PERIOD_CHARGE_5, false, null, null, null, null, false, false, null, true, null, false));
                            }
                        } else {
                            distPayHelper.saveKwtpDayLog(amount, "4.2.1 Тип распределения - для УК 14,15 при сумме недоплаты " +
                                    "<= 100");
                            distPayHelper.saveKwtpDayLog(amount, "4.2.2 Распределить оплату по вх.деб.+кред. без услуги 003, " +
                                    "по списку закрытых орг, " +
                                    "c ограничением по исх.сал.");
                            distWithRestriction.distributeMoney(
                                    new DistribParam(amount, BaseForDistPays.IN_SAL_0, true, true, true, true, true, true, false, Collections.singletonList("003"), true, null, false));
                            if (amount.isExistSumma()) {
                                distPayHelper.saveKwtpDayLog(amount, "4.2.3 Распределить по вх.деб.+кред. остальные услуги, " +
                                        "кроме списка закрытых орг. и без услуги 003 " +
                                        "с ограничением по ВХ.сальдо");
                                distWithRestriction.distributeMoney(
                                        new DistribParam(amount, BaseForDistPays.IN_SAL_0, false, null, null, null, null, false, true, Collections.singletonList("003"), true, null, true));
                            }
                            if (amount.isExistSumma()) {
                                if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                                    distPayHelper.saveKwtpDayLog(amount, "4.2.4 Остаток распределить на услугу usl=003");
                                    distExclusivelyBySingleUslIdUk(amount, "003", true);
                                }
                            }
                        }
                    }

                    if (amount.isExistSumma()) {
                        // во всех вариантах распределения остались нераспределенными средства
                        if (amount.getKart().getTp().getCd().equals("LSK_TP_MAIN")) {
                            // основной счет
                            distPayHelper.saveKwtpDayLog(amount, "4.3.1 Сумма оплаты не была распределена, распределить " +
                                    "на услугу usl=003");
                            distExclusivelyBySingleUslIdUk(amount, "003", true);
                        } else {
                            // прочие типы счетов
                            distPayHelper.saveKwtpDayLog(amount, "4.3.1 Сумма оплаты не была распределена, распределить " +
                                    "на первую услугу в наборе");
                            distExclusivelyByFirstUslId(amount, true);
                            if (amount.isExistSumma()) {
                                // если всё же осталась нераспределенная сумма, то отправить на 003 усл и УК
                                distPayHelper.saveKwtpDayLog(amount, "4.3.2 Сумма оплаты не была распределена, распределить " +
                                        "на услугу usl=003");
                                distExclusivelyBySingleUslIdUk(amount, "003", true);
                            }
                        }
                    }

                    // выполнить редирект оплаты
                    List<SumUslOrgDTO> lstDistPay = amount.getLstDistPayment();
                    lstDistPay.forEach(t -> {
                        UslOrg uslOrgChanged =
                                referenceMng.getUslOrgRedirect(t.getUslId(), t.getOrgId(), amount.getKart(), 1);
                        boolean isRedirected = false;
                        if (!t.getUslId().equals(uslOrgChanged.getUslId())) {
                            isRedirected = true;
                            distPayHelper.saveKwtpDayLog(amount, "4.4.0 Выполнено перенаправление оплаты: услуга " +
                                    "{}-->{}", t.getUslId(), uslOrgChanged.getUslId());
                            t.setUslId(uslOrgChanged.getUslId());
                        }
                        if (!t.getOrgId().equals(uslOrgChanged.getOrgId())) {
                            isRedirected = true;
                            distPayHelper.saveKwtpDayLog(amount, "4.4.1 Выполнено перенаправление оплаты: организация " +
                                    "{}-->{}", t.getOrgId(), uslOrgChanged.getOrgId());
                            t.setOrgId(uslOrgChanged.getOrgId());
                        }
                        if (isRedirected) {
                            distPayHelper.saveKwtpDayLog(amount, "4.4.2 Перенаправлена сумма={}", t.getSumma());
                        }
                    });

                } else if (amount.getSumma().compareTo(BigDecimal.ZERO) < 0) {
                    distPayHelper.saveKwtpDayLog(amount, "2.0 Сумма оплаты < 0, снятие ранее принятой оплаты");
                    // сумма оплаты < 0 (снятие оплаты)
                    throw new ErrorWhileDistPay("ОШИБКА! Сумма оплаты < 0, операция не доступна!");
                }

                if (amount.getPenya().compareTo(BigDecimal.ZERO) > 0) {
                    // распределение пени
                    distPayHelper.saveKwtpDayLog(amount, "5.0 Сумма пени > 0");
                    distPayHelper.saveKwtpDayLog(amount, "5.0.1 Распределить по уже имеющемуся распределению оплаты");
                    distWithRestriction.distributeMoney(
                            new DistribParam(amount, BaseForDistPays.ALREADY_DISTRIB_PAY_4, false, null, null, null, null, false, false, null, false, null, false));
                    if (amount.isExistPenya()) {
                        distPayHelper.saveKwtpDayLog(amount, "5.0.2 Остаток распределить по начислению");
                        distWithRestriction.distributeMoney(
                                new DistribParam(amount, BaseForDistPays.CURR_PERIOD_CHARGE_5, false, null, null, null, null, false, false, null, false, null, false));
                        if (amount.isExistPenya()) {
                            distPayHelper.saveKwtpDayLog(amount, "5.0.3 Остаток распределить по вх.сал.пени");
                            distWithRestriction.distributeMoney(
                                    new DistribParam(amount, BaseForDistPays.IN_SAL_PEN_6, false, null, null, null, null, false, false, null, false, null, false));
                        }
                        if (amount.isExistPenya()) {
                            if (amount.getKart().getTp().getCd().equals("LSK_TP_MAIN")) {
                                // основной счет
                                distPayHelper.saveKwtpDayLog(amount, "5.1.0 Сумма пени не была распределена, распределить " +
                                        "на услугу usl=003");
                                distExclusivelyBySingleUslIdUk(amount, "003", false);
                            } else {
                                // прочие типы счетов
                                distPayHelper.saveKwtpDayLog(amount, "5.1.0 Сумма пени не была распределена, распределить " +
                                        "на первую услугу в наборе");
                                distExclusivelyByFirstUslId(amount, true);
                                if (amount.isExistPenya()) {
                                    // если всё же осталась нераспределенная сумма, то отправить на 003 усл и УК
                                    distPayHelper.saveKwtpDayLog(amount, "5.1.1 Сумма пени не была распределена, распределить" +
                                            " на услугу usl=003");
                                    distExclusivelyBySingleUslIdUk(amount, "003", false);
                                }
                            }
                        }
                    }
                    // выполнить редирект пени
                    List<SumUslOrgDTO> lstDistPenya = amount.getLstDistPenya();
                    lstDistPenya.forEach(t -> {
                        UslOrg uslOrgChanged =
                                referenceMng.getUslOrgRedirect(t.getUslId(), t.getOrgId(), amount.getKart(), 0);
                        boolean isRedirected = false;
                        if (!t.getUslId().equals(uslOrgChanged.getUslId())) {
                            isRedirected = true;
                            distPayHelper.saveKwtpDayLog(amount, "5.1.0 Выполнено перенаправление пени: услуга " +
                                    "{}-->{}", t.getUslId(), uslOrgChanged.getUslId());
                            t.setUslId(uslOrgChanged.getUslId());
                        }
                        if (!t.getOrgId().equals(uslOrgChanged.getOrgId())) {
                            isRedirected = true;
                            distPayHelper.saveKwtpDayLog(amount, "5.1.1 Выполнено перенаправление пени: организация " +
                                    "{}-->{}", t.getOrgId(), uslOrgChanged.getOrgId());
                            t.setOrgId(uslOrgChanged.getOrgId());
                        }
                        if (isRedirected) {
                            distPayHelper.saveKwtpDayLog(amount, "5.1.2 Перенаправлена сумма={}", t.getSumma());
                        }
                    });

                } else if (amount.getPenya().compareTo(BigDecimal.ZERO) < 0) {
                    // сумма пени < 0 (снятие оплаты)
                    throw new ErrorWhileDistPay("ОШИБКА! Сумма пени < 0, операция не доступна!");
                }
            } else if (amount.getDistTp() == 2) {
                // ТСЖ
                if (amount.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                    distPayHelper.saveKwtpDayLog(amount, "1.0 Сумма оплаты долга > 0");
                    if (Integer.parseInt(amount.getDopl()) >= Integer.parseInt(configApp.getPeriod())) {
                        // текущий или будущий период (аванс) распределить точно по выбранным услугам,
                        // не превышая текущего начисления с учетом текущей оплаты
                        distPayHelper.saveKwtpDayLog(amount, "2.0 Период оплаты >= текущий период, распределить " +
                                "по тек.начислению списка услуг, точно");
                        distWithRestriction.distributeMoney(
                                new DistribParam(amount, BaseForDistPays.CURR_PERIOD_CHARGE_MOIFY_7, false, null, null, null, null, false, false, null, true, Arrays.asList("011", "012", "015", "016", "013", "014", "038", "039",
                                        "033", "034", "042", "044", "045"), false));
                        if (amount.isExistSumma()) {
                            // если всё еще есть остаток суммы, распределить по прочим услугам
                            distPayHelper.saveKwtpDayLog(amount, "2.1 Остаток распределить пропорционально начисления " +
                                    "прочих услуг, без ограничений");
                            distWithRestriction.distributeMoney(
                                    new DistribParam(amount, BaseForDistPays.CURR_PERIOD_CHARGE_5, false, null, null, null, null, false, false, Arrays.asList("011", "012", "015", "016", "013", "014", "038", "039",
                                            "033", "034", "042", "044", "045"), true, null, false));
                        }
                        if (amount.isExistSumma()) {
                            // если всё еще есть остаток суммы, распределить по 003 услуге
                            distPayHelper.saveKwtpDayLog(amount, "2.2 Остаток суммы распределить на услугу usl=003");
                            distExclusivelyBySingleUslIdUk(amount, "003", true);
                        }
                    } else {
                        // предыдущий период
                        distPayHelper.saveKwtpDayLog(amount, "3.1 Распределить по начислению соответствующего периода={}, " +
                                        "без ограничения по исх.сал.",
                                configApp.getPeriodBack());
                        distWithRestriction.distributeMoney(
                                new DistribParam(amount, BaseForDistPays.SELECTED_PERIOD_CHARGE_8, false, null, null, null, null, false, false, null, true, null, false));
                    }
                }

                if (amount.getPenya().compareTo(BigDecimal.ZERO) > 0) {
                    distPayHelper.saveKwtpDayLog(amount, "3.1 Сумму пени распределить на услугу usl=003");
                    distExclusivelyBySingleUslIdUk(amount, "003", false);
                }
            }
            // сгруппировать распределение оплаты
            List<SumUslOrgDTO> lstForKwtpDayPay = new ArrayList<>(20);
            amount.getLstDistPayment().forEach(t ->
                    saldoMng.groupByLstUslOrg(lstForKwtpDayPay, t));
            // сгруппировать распределение пени
            List<SumUslOrgDTO> lstForKwtpDayPenya = new ArrayList<>(20);
            amount.getLstDistPenya().forEach(t ->
                    saldoMng.groupByLstUslOrg(lstForKwtpDayPenya, t));
            Map<Integer, List<SumUslOrgDTO>> mapForKwtpDay = new HashMap<>();
            mapForKwtpDay.put(1, lstForKwtpDayPay);
            mapForKwtpDay.put(0, lstForKwtpDayPenya);

            // сохранить распределение в KWTP_DAY
            distPayHelper.saveKwtpDayLog(amount, "5.3 Итоговое, сгруппированное распределение в KWTP_DAY:");

            Map<Integer, BigDecimal> mapControl = new HashMap<>();
            mapForKwtpDay.forEach((key, lstSum) -> lstSum.forEach(d -> {
                KwtpDay kwtpDay = KwtpDay.KwtpDayBuilder.aKwtpDay()
                        .withNink(amount.getNink())
                        .withNkom(amount.getNkom())
                        .withOper(amount.getOper())
                        .withUsl(em.find(Usl.class, d.getUslId()))
                        .withOrg(em.find(Org.class, d.getOrgId()))
                        .withSumma(d.getSumma())
                        .withDopl(amount.getDopl())
                        .withDt(amount.getDtek())
                        .withDtInk(amount.getDatInk())
                        .withKart(amount.getKart())
                        .withFkKwtpMg(amount.getKwtpMgId())
                        .withTp(key).build();
                mapControl.merge(key, d.getSumma(), BigDecimal::add);
                distPayHelper.saveKwtpDayLog(amount, "tp={}, usl={}, org={}, summa={}",
                        kwtpDay.getTp(), kwtpDay.getUsl().getId(), kwtpDay.getOrg().getId(), d.getSumma());
                em.persist(kwtpDay); // note Используй crud.save
            }));


            log.info("5.4 Итого распределено Сумма={}, Пеня={}", mapControl.get(1), mapControl.get(0));
            if (amount.getSummaControl().compareTo(Utl.nvl(mapControl.get(1), BigDecimal.ZERO)) != 0
                    || amount.getSumma().compareTo(BigDecimal.ZERO) != 0) {
                throw new ErrorWhileDistPay("ОШИБКА! Не вся оплата распределена!");
            }
            if (amount.getPenyaControl().compareTo(Utl.nvl(mapControl.get(0), BigDecimal.ZERO)) != 0
                    || amount.getPenya().compareTo(BigDecimal.ZERO) != 0) {
                throw new ErrorWhileDistPay("ОШИБКА! Не вся пеня распределена!");
            }

        } catch (WrongParam e) {
            log.error(Utl.getStackTraceString(e));
            throw new ErrorWhileDistPay("Произошла ошибка в процессе распределения оплаты!");
        }

    }

    /**
     * Построить объект расчета
     */
    private Amount buildAmount(int kwtpMgId, boolean isGenChrg, String lsk,
                               String strSumma, String strPenya, String strDebt,
                               String dopl, int nink, String nkom, String oper,
                               String dtekStr, String datInkStr) throws WrongParam {
        Amount amount = new Amount();

        // тип распределения оплаты (0 - Нет, 1-Кис, 2- ТСЖ)
        amount.setDistTp(Utl.nvl(sprParamMng.getN1("JAVA_DIST_KWTP_MG"), 0D).intValue());

        Kart kart = em.find(Kart.class, lsk);
        amount.setKart(kart);
        amount.setSumma(strSumma != null && strSumma.length() > 0 ? new BigDecimal(strSumma) : BigDecimal.ZERO);
        amount.setPenya(strPenya != null && strPenya.length() > 0 ? new BigDecimal(strPenya) : BigDecimal.ZERO);
        // сохранить суммы для контроля распределения
        amount.setSummaControl(amount.getSumma());
        amount.setPenyaControl(amount.getPenya());

        amount.setAmntDebtDopl(strDebt != null && strDebt.length() > 0 ? new BigDecimal(strDebt) : BigDecimal.ZERO);
        amount.setKwtpMgId(kwtpMgId);
        amount.setDopl(dopl);
        amount.setNink(nink);
        amount.setNkom(nkom);
        amount.setOper(oper);
        try {
            amount.setDtek(Utl.getDateFromStr(dtekStr));
            amount.setDatInk(datInkStr != null && datInkStr.length() != 0 ? Utl.getDateFromStr(datInkStr) : null);
        } catch (ParseException e) {
            log.error(Utl.getStackTraceString(e));
            throw new WrongParam("ERROR! Некорректный период!");
        }

        distPayHelper.saveKwtpDayLog(amount, "***** Распределение оплаты ver=1.03 *****");
        distPayHelper.saveKwtpDayLog(amount, "1.0 C_KWTP_MG.LSK={}, C_KWTP_MG.ID={}, C_KWTP_MG.SUMMA={}, C_KWTP_MG.PENYA={}, Дата-время={}",
                kart.getLsk(), amount.getKwtpMgId(), amount.getSumma(), amount.getPenya(),
                Utl.getStrFromDate(new Date(), "dd.MM.yyyy HH:mm:ss"));
        distPayHelper.saveKwtpDayLog(amount, "УК: {}", amount.getKart().getUk().getReu());
        distPayHelper.saveKwtpDayLog(amount, "Тип счета: {}", amount.getKart().getTp().getName());
        distPayHelper.saveKwtpDayLog(amount, "Тип распределения, параметр JAVA_DIST_KWTP_MG={}", amount.getDistTp());
        distPayHelper.saveKwtpDayLog(amount, "Долг за период: {}", amount.getAmntDebtDopl());

        // сформировать начисление
        if (isGenChrg) {
                processMng.processWebRequest(CHARGE_0, 0, amount.getDtek(), null, null,
                        amount.getKart().getKoKw(), null, null);
        }

        // получить вх.общ.сал.
        amount.setInSal(saldoMng.getOutSal(amount.getKart(), configApp.getPeriod(),
                null, null,
                true, false, false, false, false, false,
                null, false, false));
        // итог по вх.сал.
        amount.setAmntInSal(amount.getInSal().stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));

        distPayHelper.saveKwtpDayLog(amount, "Итого сумма вх.деб.+кред.сал={}", amount.getAmntInSal());
        // получить начисление за прошлый период
        List<SumUslOrgDTO> lstChrgPrevPeriod =
                saldoMng.getOutSal(amount.getKart(), configApp.getPeriod(),
                        null, null,
                        false, false, true, false, false, false,
                        configApp.getPeriodBack(), false, false);
        // получить итог начисления за прошлый период
        amount.setAmntChrgPrevPeriod(lstChrgPrevPeriod
                .stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        distPayHelper.saveKwtpDayLog(amount, "Итого сумма начисления за прошлый период={}", amount.getAmntChrgPrevPeriod());

        /*distPayHelper.saveKwtpDayLog(amount.getKwtpMg(),"Вх.сальдо по лиц.счету lsk={}:",
                amount.getKart().getLsk());
        amount.getInSal().forEach(t -> distPayHelper.saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}",
                t.getUslId(), t.getOrgId(), t.getSumma()));
        amount.setLstSprProcPay(sprProcPayDAO.findAll());
        distPayHelper.saveKwtpDayLog(amount.getKwtpMg(),"итого:{}", amount.getAmntInSal());
        distPayHelper.saveKwtpDayLog(amount.getKwtpMg(),"");*/

        // получить вх.деб.сал.
/*        amount.setInDebSal(amount.getInSal()
                .stream().filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0)
                .collect(Collectors.toList()));
        // итог по вх.деб.сал.
        amount.setAmntInDebSal(amount.getInDebSal().stream().map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        distPayHelper.saveKwtpDayLog(amount.getKwtpMg(),"Вх.деб.сальдо по лиц.счету lsk={}:",
                amount.getKart().getLsk());
        amount.getInDebSal().forEach(t -> distPayHelper.saveKwtpDayLog(amount.getKwtpMg(),"usl={}, org={}, summa={}",
                t.getUslId(), t.getOrgId(), t.getSumma()));*/
        amount.setLstSprProcPay(sprProcPayDAO.findAll());
        //distPayHelper.saveKwtpDayLog(amount.getKwtpMg(),"итого:{}", amount.getAmntInDebSal());
        return amount;
    }


    /**
     * Распределить платеж экслюзивно, на одну услугу
     *
     * @param amount           - итоги
     * @param includeOnlyUslId - список Id услуг, на которые распределить оплату, не зависимо от их сальдо
     * @param isDistPay        - распределять оплату - да, пеню - нет
     */
/*
    private void distExclusivelyBySingleUslId(Amount amount, String includeOnlyUslId, boolean isDistPay) throws ErrorWhileDistPay {
        // Распределить эксклюзивно на одну услугу
        Nabor nabor = naborDAO.getByLskUsl(amount.getKart().getLsk(), includeOnlyUslId);
        if (nabor != null) {
            // сохранить для записи в KWTP_DAY
            BigDecimal distSumma;
            if (isDistPay) {
                distSumma = amount.getSumma();
                amount.getLstDistPayment().add(
                        new SumUslOrgDTO(includeOnlyUslId, nabor.getOrg().getId(), distSumma));
                amount.setSumma(amount.getSumma().add(distSumma.negate()));
            } else {
                distSumma = amount.getPenya();
                amount.getLstDistPenya().add(
                        new SumUslOrgDTO(includeOnlyUslId, nabor.getOrg().getId(), distSumma));
                amount.setPenya(amount.getPenya().add(distSumma.negate()));
            }
            distPayHelper.saveKwtpDayLog(amount, "Распределено фактически:");
            distPayHelper.saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                    includeOnlyUslId, nabor.getOrg().getId(), distSumma);
            distPayHelper.saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                    distSumma, amount.getSumma());
        } else {
            throw new ErrorWhileDistPay("При распределении не найдена запись в наборе услуг lsk="
                    + amount.getKart().getLsk()
                    + ", usl=" + includeOnlyUslId);
        }
    }
*/

    /**
     * Распределить платеж экслюзивно, на первую услугу в наборе
     *
     * @param amount    - итоги
     * @param isDistPay - распределять оплату - да, пеню - нет
     */
    private void distExclusivelyByFirstUslId(Amount amount, boolean isDistPay) {
        Nabor nabor = amount.getKart().getNabor().stream().findFirst().orElse(null);
        if (nabor != null) {
            // сохранить для записи в KWTP_DAY
            BigDecimal distSumma;
            if (isDistPay) {
                distSumma = amount.getSumma();
                amount.getLstDistPayment().add(
                        new SumUslOrgDTO(nabor.getUsl().getId(), nabor.getOrg().getId(), distSumma));
                amount.setSumma(amount.getSumma().add(distSumma.negate()));
            } else {
                distSumma = amount.getPenya();
                amount.getLstDistPenya().add(
                        new SumUslOrgDTO(nabor.getUsl().getId(), nabor.getOrg().getId(), distSumma));
                amount.setPenya(amount.getPenya().add(distSumma.negate()));
            }
            distPayHelper.saveKwtpDayLog(amount, "Распределено фактически:");
            distPayHelper.saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                    nabor.getUsl().getId(), nabor.getOrg().getId(), distSumma);
            distPayHelper.saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                    distSumma, amount.getSumma());
        }
    }

    /**
     * Распределить платеж экслюзивно, на одну услугу и УК лицевого счета
     *
     * @param amount           - итоги
     * @param includeOnlyUslId - список Id услуг, на которые распределить оплату, не зависимо от их сальдо
     * @param isDistPay        - распределять оплату - да, пеню - нет
     */
    private void distExclusivelyBySingleUslIdUk(Amount amount, String includeOnlyUslId,
                                                boolean isDistPay) {
        // сохранить для записи в KWTP_DAY
        BigDecimal distSumma;
        if (isDistPay) {
            distSumma = amount.getSumma();
            amount.getLstDistPayment().add(
                    new SumUslOrgDTO(includeOnlyUslId, amount.getKart().getUk().getId(), distSumma));
            amount.setSumma(amount.getSumma().add(distSumma.negate()));
        } else {
            distSumma = amount.getPenya();
            amount.getLstDistPenya().add(
                    new SumUslOrgDTO(includeOnlyUslId, amount.getKart().getUk().getId(), distSumma));
            amount.setPenya(amount.getPenya().add(distSumma.negate()));
        }
        distPayHelper.saveKwtpDayLog(amount, "Распределено фактически:");
        distPayHelper.saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                includeOnlyUslId, amount.getKart().getUk().getId(), distSumma);
        distPayHelper.saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                distSumma, amount.getSumma());
    }

    /**
     * Выполняемая раз в месяц коррекционная проводка по сальдо
     */
    @Override
    public void distSalCorrOperation() {
        //configApp.getPeriod();
        saldoUslDAO.getSaldoUslBySign("201404", 1)
                .forEach(t -> log.info("SaldoUsl: lsk={}, usl={}, org={}, summa={}",
                        t.getKart().getLsk(), t.getUsl().getId(),
                        t.getOrg().getId(), t.getSumma()));


    }
}
