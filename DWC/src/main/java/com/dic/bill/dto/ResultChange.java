package com.dic.bill.dto;

import lombok.*;

import java.math.BigDecimal;

@Value
@Builder
public class ResultChange {
    String lsk;
    String mg;
    String uslId;
    Integer org1Id;
    BigDecimal proc1;
    BigDecimal summa;
}
