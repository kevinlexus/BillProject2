package com.dic.bill.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class SumChargeRec implements SumCharge {
    private String name;
    private Integer npp;
    private Double vol;
    private Double price;
    private String unit;
    private Double summa;
}
