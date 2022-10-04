package com.dic.app.service.impl;

import com.dic.app.enums.BaseForDistPays;
import com.dic.app.service.ConfigApp;
import com.dic.bill.dto.Amount;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.mm.SaldoMng;
import com.ric.cmn.DistributableBigDecimal;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongParam;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class DistPayWithRestriction {

    private final ConfigApp configApp;
    private final DistPayHelper distPayHelper;
    private final SaldoMng saldoMng;

    /**
     * Распределить платеж
     * @param distribParam
     */
    public void distributeMoney(DistribParam distribParam) throws WrongParam {
        if (!distribParam.isRestrictByOutSal() && distribParam.getUseChargeInRestrict() != null && distribParam.getUseChangeInRestrict() != null
                && distribParam.getUseCorrPayInRestrict() != null && distribParam.getUsePayInRestrict() != null) {
            throw new WrongParam("Некорректно заполнять isUseChargeInRestrict, isUseChangeInRestrict, " +
                    "isUseCorrPayInRestrict, isUsePayInRestrict при isRestrictByOutSal=false!");
        } else if (distribParam.isRestrictByOutSal() && distribParam.isRestrictByInSal()) {
            throw new WrongParam("Некорректно использовать совместно isRestrictByOutSal=true и isRestrictByInSal=true!");
        } else if (distribParam.isRestrictByInSal() && (distribParam.getUseChargeInRestrict() != null || distribParam.getUseChangeInRestrict() != null
                || distribParam.getUseCorrPayInRestrict() != null || distribParam.getUsePayInRestrict() != null)) {
            throw new WrongParam("Некорректно использовать совместно isRestrictByInSal=true и isUseChargeInRestrict, " +
                    "isUseChangeInRestrict, isUseCorrPayInRestrict, isUsePayInRestrict!");
        } else if (distribParam.isRestrictByOutSal() && (distribParam.getUseChargeInRestrict() == null && distribParam.getUseChangeInRestrict() == null
                && distribParam.getUseCorrPayInRestrict() == null && distribParam.getUsePayInRestrict() == null)) {
            throw new WrongParam("Не заполнено isUseChargeInRestrict, isUseChangeInRestrict, " +
                    "isUseCorrPayInRestrict, isUsePayInRestrict при isRestrictByOutSal=true!");
        }

        // Распределить на все услуги
        String currPeriod = configApp.getPeriod();
        List<SumUslOrgDTO> lstDistribBase;
        // получить базовую коллекцию для распределения
        lstDistribBase = distPayHelper.getBaseForDistrib(distribParam.getAmount(), distribParam.getTp(), distribParam.isIncludeByClosedOrgList(), distribParam.isExcludeByClosedOrgList(), distribParam.getLstExcludeUslId(), distribParam.getLstFilterByUslId(), currPeriod);


        Map<DistributableBigDecimal, BigDecimal> mapDistPay = null;
        if (distribParam.getTp().equals(BaseForDistPays.CURR_PERIOD_CHARGE_MOIFY_7)) {
            // распределить сумму в точности по услугам
            if (distribParam.isDistPay()) {
                // распределить оплату
                mapDistPay = Utl.distBigDecimalPositiveByListIntoMapExact(distribParam.getAmount().getSumma(), lstDistribBase);
            } else {
                // распределить пеню (не должна быть в данном типе распр, но вдруг будет)
                mapDistPay = Utl.distBigDecimalPositiveByListIntoMapExact(distribParam.getAmount().getPenya(), lstDistribBase);
            }
        } else {
            // прочие типы распределения
            // распределить сумму по базе распределения
            if (distribParam.isDistPay()) {
                // распределить оплату
                mapDistPay =
                        Utl.distBigDecimalByListIntoMap(distribParam.getAmount().getSumma(), lstDistribBase, 2);
            } else {
                // распределить пеню
                mapDistPay =
                        Utl.distBigDecimalByListIntoMap(distribParam.getAmount().getPenya(), lstDistribBase, 2);
            }
        }

        // распечатать предварительное распределение оплаты или сохранить, если не будет ограничения по сальдо
        BigDecimal distSumma = saveDistPay(distribParam.getAmount(), mapDistPay, !(distribParam.isRestrictByOutSal() || distribParam.isRestrictByInSal()), distribParam.isDistPay());

        if (distribParam.isRestrictByOutSal() || distribParam.isRestrictByInSal()) {
            if (distribParam.isRestrictByOutSal()) {
                distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "Сумма для распределения будет ограничена по исх.сальдо");
            } else {
                distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "Сумма для распределения будет ограничена по вх.сальдо");
            }
            if (distSumma.compareTo(BigDecimal.ZERO) != 0) {
                // Ограничить суммы  распределения по услугам, чтобы не было кредитового сальдо
                // получить сумму исходящего сальдо, учитывая все операции
                List<SumUslOrgDTO> lstOutSal = null;
                if (distribParam.isRestrictByOutSal()) {
                    lstOutSal = saldoMng.getOutSal(distribParam.getAmount().getKart(), currPeriod,
                            distribParam.getAmount().getLstDistPayment(), distribParam.getAmount().getLstDistControl(),
                            true, distribParam.getUseChargeInRestrict(), false, distribParam.getUseChangeInRestrict(),
                            distribParam.getUseCorrPayInRestrict(),
                            distribParam.getUsePayInRestrict(), null, false, false);
                } else {
                    lstOutSal = saldoMng.getOutSal(distribParam.getAmount().getKart(), currPeriod,
                            distribParam.getAmount().getLstDistPayment(), distribParam.getAmount().getLstDistControl(),
                            true, false, false, false,
                            false,false, null, false, false);
                }
                if (distribParam.isRestrictByOutSal()) {
                    distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "Исх.сальдо по лиц.счету lsk={}:",
                            distribParam.getAmount().getKart().getLsk());
                } else {
                    distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "Вх.сальдо по лиц.счету lsk={}:",
                            distribParam.getAmount().getKart().getLsk());
                }
                lstOutSal.forEach(t -> distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));

                // ограничить суммы распределения по исх. или вх. сал.
                for (Map.Entry<DistributableBigDecimal, BigDecimal> dist : mapDistPay.entrySet()) {
                    SumUslOrgDTO distRec = (SumUslOrgDTO) dist.getKey();
                    if (dist.getValue().compareTo(BigDecimal.ZERO) != 0) {
                        // контролировать по значениям распределения
                        lstOutSal.stream().filter(sal -> sal.getUslId().equals(distRec.getUslId())
                                        && sal.getOrgId().equals(distRec.getOrgId()))
                                .forEach(sal -> {
                                    if (dist.getValue().compareTo(BigDecimal.ZERO) > 0) {
                                        // ограничить положительные суммы распределения, если появилось кред.сал.
                                        if (sal.getSumma().compareTo(BigDecimal.ZERO) < 0) {
                                            if (sal.getSumma().abs().compareTo(dist.getValue()) > 0) {
                                                // кредит сальдо больше распред.суммы в абс выражении
                                                dist.setValue(BigDecimal.ZERO);
                                            } else {
                                                // кредит сальдо меньше распред.суммы в абс выражении
                                                dist.setValue(dist.getValue().subtract(sal.getSumma().abs()));
                                            }
                                        }
                                    } else {
                                        // ограничить отрицательные суммы распределения, если появилось деб.сал.
                                        if (sal.getSumma().compareTo(BigDecimal.ZERO) > 0) {
                                            if (sal.getSumma().compareTo(dist.getValue().abs()) > 0) {
                                                // деб.сальдо больше распред.суммы в абс выражении
                                                dist.setValue(BigDecimal.ZERO);
                                            } else {
                                                // деб.сальдо меньше распред.суммы в абс выражении
                                                dist.setValue(dist.getValue().abs().subtract(sal.getSumma()).negate());
                                            }
                                        }
                                    }

                                    if (distribParam.isRestrictByOutSal()) {
                                        distPayHelper.saveKwtpDayLog(distribParam.getAmount(),
                                                "распределение ограничено по исх.сал. usl={}, org={}, summa={}",
                                                sal.getUslId(), sal.getOrgId(), dist.getValue());
                                    } else {
                                        distPayHelper.saveKwtpDayLog(distribParam.getAmount(),
                                                "распределение ограничено по вх.сал. usl={}, org={}, summa={}",
                                                sal.getUslId(), sal.getOrgId(), dist.getValue());
                                    }
                                });
                    }
                }

                // сохранить, распечатать распределение оплаты
                distSumma = saveDistPay(distribParam.getAmount(), mapDistPay, true, distribParam.isDistPay());
                // получить, распечатать исходящее сальдо
                lstOutSal = saldoMng.getOutSal(distribParam.getAmount().getKart(), currPeriod,
                        distribParam.getAmount().getLstDistPayment(), distribParam.getAmount().getLstDistPayment(),
                        true, true, false, true,
                        true, true, null, false, false);
                if (distribParam.isDistPay()) {
                    distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "После распределения оплаты: Исх.сальдо по лиц.счету lsk={}:",
                            distribParam.getAmount().getKart().getLsk());
                }
                lstOutSal.forEach(t -> distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));
            }
        }

        // вычесть из итоговой суммы платежа
        if (distribParam.isDistPay()) {
            distribParam.getAmount().setSumma(distribParam.getAmount().getSumma().add(distSumma.negate()));
            distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "итого распределено:{}, остаток:{}",
                    distSumma, distribParam.getAmount().getSumma());
        } else {
            distribParam.getAmount().setPenya(distribParam.getAmount().getPenya().add(distSumma.negate()));
            distPayHelper.saveKwtpDayLog(distribParam.getAmount(), "итого распределено:{}, остаток:{}",
                    distSumma, distribParam.getAmount().getPenya());
        }


    }


    /**
     * Сохранить, суммировать и распечатать распределение
     *
     * @param amount     - Итоги
     * @param mapDistPay - коллекция распределения
     * @param isSave     - сохранить распределение?
     * @param isDistPay  - распределить оплату - да, пеню - нет
     */
    private BigDecimal saveDistPay(Amount amount, Map<DistributableBigDecimal, BigDecimal> mapDistPay,
                                   boolean isSave, boolean isDistPay) {
        String msg = "оплаты";
        if (!isDistPay) {
            msg = "пени";
        }
        if (isSave) {
            distPayHelper.saveKwtpDayLog(amount, "Распределено {}, фактически:", msg);
        } else {
            distPayHelper.saveKwtpDayLog(amount, "Распределено {}, предварительно:", msg);
        }
        BigDecimal amnt = BigDecimal.ZERO;
        for (Map.Entry<DistributableBigDecimal, BigDecimal> t : mapDistPay.entrySet()) {
            SumUslOrgDTO sumUslOrgDTO = (SumUslOrgDTO) t.getKey();
            if (isSave) {
                // сохранить для записи в KWTP_DAY
                if (isDistPay) {
                    // оплата
                    amount.getLstDistPayment().add(
                            new SumUslOrgDTO(sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue()));
                } else {
                    // пеня
                    amount.getLstDistPenya().add(
                            new SumUslOrgDTO(sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue()));
                }
            } else {
                // сохранить для контроля
                amount.getLstDistControl().add(
                        new SumUslOrgDTO(sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue()));
            }
            amnt = amnt.add(t.getValue());
            distPayHelper.saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                    sumUslOrgDTO.getUslId(), sumUslOrgDTO.getOrgId(), t.getValue());
        }

        distPayHelper.saveKwtpDayLog(amount, "итого {}, распределено:{}", msg, amnt);
        return amnt;
    }
}
