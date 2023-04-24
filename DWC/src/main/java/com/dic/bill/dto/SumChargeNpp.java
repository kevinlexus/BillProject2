package com.dic.bill.dto;

/*
 * Projection для хранения записи начисления
 */
public interface SumChargeNpp {
    String getId(); // Id услуги
    String getName();

    Integer getNpp();

    Double getVol();

    Double getPrice();

    String getUnit();

    Double getSumma();
}

