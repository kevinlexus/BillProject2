package com.ric.dto;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true)
@AllArgsConstructor()
public class MeterVal {
    // лиц.счет
    String lsk;
    // Id пользователя
    int userId;
    // Id реестра
    int docParId;
    // установить предыдущее показание? ВНИМАНИЕ! Текущие введёные показания будут сброшены назад
    boolean isSetPreviousVal;
    // код услуги (usl.usl)
    String codeUsl;
    // текущее значение
    Double curVal;
    // предыдущее значение (если установлено)
    Double prevVal;
}
