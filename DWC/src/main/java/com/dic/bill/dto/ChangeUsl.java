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
    Integer orgId;
    BigDecimal proc;
    BigDecimal absSet;
    Integer cntDays;
}
