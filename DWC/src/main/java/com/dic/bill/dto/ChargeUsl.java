package com.dic.bill.dto;

import org.springframework.beans.factory.annotation.Value;

import java.math.BigDecimal;

public interface ChargeUsl {
    String getMgFrom();

    String getMgTo();

    @Value("#{target.usl.id}")
    String getUslId();

    BigDecimal getSumma();
}
