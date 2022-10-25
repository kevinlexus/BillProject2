package com.dic.bill.dto;

/*
 * Projection для хранения записи движения
 */
public interface SumFinanceFlow {
    // период задолженности
    String getMg();

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

