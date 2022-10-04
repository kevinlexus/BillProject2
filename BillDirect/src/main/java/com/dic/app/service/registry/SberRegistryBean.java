package com.dic.app.service.registry;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class SberRegistryBean {

    public static final String[] FLDS = new String[]{"dt", "time", "department", "cashier", "operation", "ls", "fio",
            "address", "period", "meter", "serviceCode", "serviceName", "payment", "serviceFlagEnd", "amountPay",
            "amountSend", "commission"};
    String dt;
    String time;
    String department;
    String cashier;
    String operation;
    String ls;
    String fio;
    String address;
    String period;
    String meter;
    String serviceCode;
    String serviceName;
    String payment;
    String serviceFlagEnd;
    String amountPay;
    String amountSend;
    String commission;
}
