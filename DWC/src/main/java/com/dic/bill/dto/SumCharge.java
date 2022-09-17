package com.dic.bill.dto;

/*
 * Projection для хранения записи начисления
 */
public interface SumCharge {
    // услуга
    String getName();

    // объем
    Double getVol();

    // цена
    Double getPrice();

    // ед.измерения
    String getUnit();

    // сумма начисления
    Double getSumma();


}

