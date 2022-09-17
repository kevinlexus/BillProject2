package com.dic.app.telegram.bot.mng.impl;

enum Buttons {
    METER_BACK("selectedMeterBack", "Назад"),
    INPUT_BACK("selectedInputBack", "Назад"),
    BILLING("selectedBilling", "Отчеты"),
    BILLING_CHARGES("selectedBillingCurrentCharges", "Текущее начисление"),
    BILLING_PAYMENTS("selectedBillingPayments", "Поступление платежей"),
    BILLING_BACK("selectedBillingBack", "Назад"),
    ADDRESS_KLSK("selectedKlsk", ""),
    METER("selectedMeter", "");

    private final String name;
    private final String callBackData;

    Buttons(String callBackData, String name) {
        this.name = name;
        this.callBackData = callBackData;
    }

    public String getCallBackData() {
        return callBackData;
    }

    @Override
    public String toString() {
        return name;
    }
}
