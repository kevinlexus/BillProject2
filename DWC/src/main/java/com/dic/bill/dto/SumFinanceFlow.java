package com.dic.bill.dto;

/*
 * Projection для хранения записи движения
 */
public interface SumFinanceFlow {
    Integer getPeriod();

    // долг
    Double getDebt();

    // пеня
    Double getPen();

    // начисление
    Double getChrg();

    // оплата
    Double getPay();

    // оплата пени
    Double getPayPen();

}

