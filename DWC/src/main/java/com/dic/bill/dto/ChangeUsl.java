package com.dic.bill.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Value;

import java.math.BigDecimal;

@NoArgsConstructor
@Getter
@Setter
public class ChangeUsl {
    String uslId;
    Integer org1Id;
    BigDecimal proc1;
    Integer org2Id;
    BigDecimal proc2;
    BigDecimal absSet;
    Integer cntDays;
    Integer cntDays2;
}
