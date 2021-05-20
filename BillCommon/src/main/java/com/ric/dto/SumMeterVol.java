package com.ric.dto;

import java.math.BigDecimal;
import java.util.Date;

/*
 * Projection для хранения суммарных объемов по счетчикам
 * @author - Lev
 * @ver 1.00
 */
public interface SumMeterVol {

    // Id счетчика
    Integer getMeterId();

    // код услуги
    String getUslId();

    // начало действия счетчика
    Date getDtFrom();

    // окончание действия счетчика
    Date getDtTo();

    // объем
    BigDecimal getVol();

    // последнее показание
    BigDecimal getN1();

    // наименование услуги
    String getServiceName();
}
