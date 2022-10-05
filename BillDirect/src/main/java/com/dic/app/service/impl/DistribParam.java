package com.dic.app.service.impl;

import com.dic.app.enums.BaseForDistPays;
import com.dic.bill.dto.Amount;

import java.util.List;

public class DistribParam {
    private final Amount amount;
    private final BaseForDistPays tp;
    private final boolean isRestrictByOutSal;
    private final Boolean isUseChargeInRestrict;
    private final Boolean isUseChangeInRestrict;
    private final Boolean isUseCorrPayInRestrict;
    private final Boolean isUsePayInRestrict;
    private final boolean isIncludeByClosedOrgList;
    private final boolean isExcludeByClosedOrgList;
    private final List<String> lstExcludeUslId;
    private final boolean isDistPay;
    private final List<String> lstFilterByUslId;
    private final boolean isRestrictByInSal;

    /**
     * @param amount                   - итоги
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
    public DistribParam(Amount amount, BaseForDistPays tp, boolean isRestrictByOutSal, Boolean isUseChargeInRestrict,
                        Boolean isUseChangeInRestrict, Boolean isUseCorrPayInRestrict, Boolean isUsePayInRestrict,
                        boolean isIncludeByClosedOrgList, boolean isExcludeByClosedOrgList, List<String> lstExcludeUslId,
                        boolean isDistPay, List<String> lstFilterByUslId, boolean isRestrictByInSal) {
        this.amount = amount;
        this.tp = tp;
        this.isRestrictByOutSal = isRestrictByOutSal;
        this.isUseChargeInRestrict = isUseChargeInRestrict;
        this.isUseChangeInRestrict = isUseChangeInRestrict;
        this.isUseCorrPayInRestrict = isUseCorrPayInRestrict;
        this.isUsePayInRestrict = isUsePayInRestrict;
        this.isIncludeByClosedOrgList = isIncludeByClosedOrgList;
        this.isExcludeByClosedOrgList = isExcludeByClosedOrgList;
        this.lstExcludeUslId = lstExcludeUslId;
        this.isDistPay = isDistPay;
        this.lstFilterByUslId = lstFilterByUslId;
        this.isRestrictByInSal = isRestrictByInSal;
    }

    public Amount getAmount() {
        return amount;
    }

    public BaseForDistPays getTp() {
        return tp;
    }

    public boolean isRestrictByOutSal() {
        return isRestrictByOutSal;
    }

    public Boolean getUseChargeInRestrict() {
        return isUseChargeInRestrict;
    }

    public Boolean getUseChangeInRestrict() {
        return isUseChangeInRestrict;
    }

    public Boolean getUseCorrPayInRestrict() {
        return isUseCorrPayInRestrict;
    }

    public Boolean getUsePayInRestrict() {
        return isUsePayInRestrict;
    }

    public boolean isIncludeByClosedOrgList() {
        return isIncludeByClosedOrgList;
    }

    public boolean isExcludeByClosedOrgList() {
        return isExcludeByClosedOrgList;
    }

    public List<String> getLstExcludeUslId() {
        return lstExcludeUslId;
    }

    public boolean isDistPay() {
        return isDistPay;
    }

    public List<String> getLstFilterByUslId() {
        return lstFilterByUslId;
    }

    public boolean isRestrictByInSal() {
        return isRestrictByInSal;
    }
}
