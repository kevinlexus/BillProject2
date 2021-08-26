package com.dic.bill.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class UnloadPaymentParameter {
    private final Integer orgId;
    private final Date genDt1;
    private final Date genDt2;
    private String fileName;
    private final String ordNum;

}
