package com.dic.bill.dto;

/*
 * Projection для хранения записи начисления
 */
public interface SumChargeNpp {
    // услуга
    String getName();

    // № п.п
    Integer getNpp();

    // объем
    Double getVol();

    // цена
    Double getPrice();

    // ед.измерения
    String getUnit();

    // сумма начисления
    Double getSumma();


}

