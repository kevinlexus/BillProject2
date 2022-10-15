package com.dic.bill.dto;

import java.util.Date;

/*
 * Projection для хранения записи платежа
 */
public interface SumPayment {
    Date getDt(); // дата платежа

    Double getSumma(); // сумма

    String getSource(); // источник поступления
}

