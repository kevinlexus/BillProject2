package com.dic.bill.dto;

import lombok.*;

import java.math.BigDecimal;

@Value
@Builder
public class ResultChange {
    String lsk;
    String mg;
    String uslId;
    Integer orgId;
    BigDecimal proc;
    BigDecimal summa;
}
