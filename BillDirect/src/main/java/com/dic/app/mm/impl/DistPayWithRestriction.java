package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
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
     *  @param amount                   - итоги
     * @param tp                       - тип 0-по вх.деб.сал.+кред.сал, 1- по начислению,
     *                                 2- по деб.сал-оплата, 3 -по начислению предыдущего периода
     *                                 4- по уже готовому распределению оплаты долга (распр.пени обычно)
     *                                 5- по начислению текущего периода,
     *                                 6- распределение пени по вх.саль.до по пене
     *                                 7- распределение оплаты точно, без округлений по списку услуг
     * @param isRestrictByOutSal       - ограничить по исх.сал. (проверять чтобы не создавалось кред.сальдо)?
     * @param isUseChargeInRestrict    - использовать в ограничении по исх.деб.сал.начисление?
     * @param isUseChangeInRestrict    - использовать в ограничении по исх.деб.сал.перерасчеты?
     * @param isUseCorrPayInRestrict   - использовать в ограничении по исх.деб.сал.корр оплаты?
     * @param isUsePayInRestrict       - использовать в ограничении по исх.деб.сал.оплату?
     * @param isIncludeByClosedOrgList - включая услуги и организации по списку закрытых организаций?
     * @param isExcludeByClosedOrgList - исключая услуги и организации по списку закрытых организаций?
     * @param lstExcludeUslId          - список Id услуг, которые исключить из базовой коллекции для распределения
     * @param isDistPay                - распределять оплату - да, пеню - нет
     * @param lstFilterByUslId         - профильтровать по списку Id услуг
     * @param isRestrictByInSal        - ограничить по исх.сал. (проверять чтобы не создавалось кред.сальдо)?
     */
    public void distributeMoney(Amount amount, int tp, boolean isRestrictByOutSal,
                                Boolean isUseChargeInRestrict, Boolean isUseChangeInRestrict,
                                Boolean isUseCorrPayInRestrict, Boolean isUsePayInRestrict,
                                boolean isIncludeByClosedOrgList,
                                boolean isExcludeByClosedOrgList, List<String> lstExcludeUslId,
                                boolean isDistPay,
                                List<String> lstFilterByUslId, boolean isRestrictByInSal) throws WrongParam {
        if (!isRestrictByOutSal && isUseChargeInRestrict != null && isUseChangeInRestrict != null
                && isUseCorrPayInRestrict != null && isUsePayInRestrict != null) {
            throw new WrongParam("Некорректно заполнять isUseChargeInRestrict, isUseChangeInRestrict, " +
                    "isUseCorrPayInRestrict, isUsePayInRestrict при isRestrictByOutSal=false!");
        } else if (isRestrictByOutSal && isRestrictByInSal) {
            throw new WrongParam("Некорректно использовать совместно isRestrictByOutSal=true и isRestrictByInSal=true!");
        } else if (isRestrictByInSal  && (isUseChargeInRestrict != null || isUseChangeInRestrict != null
                || isUseCorrPayInRestrict != null || isUsePayInRestrict != null)) {
            throw new WrongParam("Некорректно использовать совместно isRestrictByInSal=true и isUseChargeInRestrict, " +
                    "isUseChangeInRestrict, isUseCorrPayInRestrict, isUsePayInRestrict!");
        } else if (isRestrictByOutSal && (isUseChargeInRestrict == null && isUseChangeInRestrict == null
                && isUseCorrPayInRestrict == null && isUsePayInRestrict == null)) {
            throw new WrongParam("Не заполнено isUseChargeInRestrict, isUseChangeInRestrict, " +
                    "isUseCorrPayInRestrict, isUsePayInRestrict при isRestrictByOutSal=true!");
        }

        // Распределить на все услуги
        String currPeriod = configApp.getPeriod();
        List<SumUslOrgDTO> lstDistribBase;
        // получить базовую коллекцию для распределения
        lstDistribBase = distPayHelper.getBaseForDistrib(amount, tp, isIncludeByClosedOrgList, isExcludeByClosedOrgList, lstExcludeUslId, lstFilterByUslId, currPeriod);


        Map<DistributableBigDecimal, BigDecimal> mapDistPay = null;
        if (tp == 7) {
            // распределить сумму в точности по услугам
            if (isDistPay) {
                // распределить оплату
                mapDistPay = Utl.distBigDecimalPositiveByListIntoMapExact(amount.getSumma(), lstDistribBase);
            } else {
                // распределить пеню (не должна быть в данном типе распр, но вдруг будет)
                mapDistPay = Utl.distBigDecimalPositiveByListIntoMapExact(amount.getPenya(), lstDistribBase);
            }
        } else {
            // прочие типы распределения
            // распределить сумму по базе распределения
            if (isDistPay) {
                // распределить оплату
                mapDistPay =
                        Utl.distBigDecimalByListIntoMap(amount.getSumma(), lstDistribBase, 2);
            } else {
                // распределить пеню
                mapDistPay =
                        Utl.distBigDecimalByListIntoMap(amount.getPenya(), lstDistribBase, 2);
            }
        }

        // распечатать предварительное распределение оплаты или сохранить, если не будет ограничения по сальдо
        BigDecimal distSumma = saveDistPay(amount, mapDistPay, !(isRestrictByOutSal || isRestrictByInSal), isDistPay);

        if (isRestrictByOutSal || isRestrictByInSal) {
            if (isRestrictByOutSal) {
                distPayHelper.saveKwtpDayLog(amount, "Сумма для распределения будет ограничена по исх.сальдо");
            } else {
                distPayHelper.saveKwtpDayLog(amount, "Сумма для распределения будет ограничена по вх.сальдо");
            }
            if (distSumma.compareTo(BigDecimal.ZERO) != 0) {
                // Ограничить суммы  распределения по услугам, чтобы не было кредитового сальдо
                // получить сумму исходящего сальдо, учитывая все операции
                List<SumUslOrgDTO> lstOutSal = null;
                if (isRestrictByOutSal) {
                    lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                            amount.getLstDistPayment(), amount.getLstDistControl(),
                            true, isUseChargeInRestrict, false, isUseChangeInRestrict,
                            isUseCorrPayInRestrict,
                            isUsePayInRestrict, null, false, false);
                } else {
                    lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                            amount.getLstDistPayment(), amount.getLstDistControl(),
                            true, false, false, false,
                            false,false, null, false, false);
                }
                if (isRestrictByOutSal) {
                    distPayHelper.saveKwtpDayLog(amount, "Исх.сальдо по лиц.счету lsk={}:",
                            amount.getKart().getLsk());
                } else {
                    distPayHelper.saveKwtpDayLog(amount, "Вх.сальдо по лиц.счету lsk={}:",
                            amount.getKart().getLsk());
                }
                lstOutSal.forEach(t -> distPayHelper.saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
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

                                    if (isRestrictByOutSal) {
                                        distPayHelper.saveKwtpDayLog(amount,
                                                "распределение ограничено по исх.сал. usl={}, org={}, summa={}",
                                                sal.getUslId(), sal.getOrgId(), dist.getValue());
                                    } else {
                                        distPayHelper.saveKwtpDayLog(amount,
                                                "распределение ограничено по вх.сал. usl={}, org={}, summa={}",
                                                sal.getUslId(), sal.getOrgId(), dist.getValue());
                                    }
                                });
                    }
                }

                // сохранить, распечатать распределение оплаты
                distSumma = saveDistPay(amount, mapDistPay, true, isDistPay);
                // получить, распечатать исходящее сальдо
                lstOutSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                        amount.getLstDistPayment(), amount.getLstDistPayment(),
                        true, true, false, true,
                        true, true, null, false, false);
                if (isDistPay) {
                    distPayHelper.saveKwtpDayLog(amount, "После распределения оплаты: Исх.сальдо по лиц.счету lsk={}:",
                            amount.getKart().getLsk());
                }
                lstOutSal.forEach(t -> distPayHelper.saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));
            }
        }

        // вычесть из итоговой суммы платежа
        if (isDistPay) {
            amount.setSumma(amount.getSumma().add(distSumma.negate()));
            distPayHelper.saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                    distSumma, amount.getSumma());
        } else {
            amount.setPenya(amount.getPenya().add(distSumma.negate()));
            distPayHelper.saveKwtpDayLog(amount, "итого распределено:{}, остаток:{}",
                    distSumma, amount.getPenya());
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
