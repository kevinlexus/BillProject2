package com.dic.bill.dto;

import lombok.Value;

import java.util.List;

/**
 * Результат выполнения потока
 *
 * @author Lev
 */
@Value
public class CommonResult {

    List<LskChargeUsl> lskChargeUsls;

}
