package com.dic.bill.dto;

/*
 * Projection для хранения записи начисления
 */
public interface SumChangeNpp {
    String getId(); // Id услуги
    String getName();

    Integer getNpp();

    Double getVol();

    String getUnit();

    Double getSumma();

}

