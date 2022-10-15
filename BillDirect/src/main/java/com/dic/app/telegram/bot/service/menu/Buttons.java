package com.dic.app.telegram.bot.service.menu;

public enum Buttons {
    BACK("selectedBack", "Назад"),
    REPORTS("selectedReport", "Отчеты"),
    BILLING_FLOW("selectedBillingFlow", "Движение средств"),
    BILLING_CHARGES("selectedBillingCurrentCharges", "Текущее начисление"),
    BILLING_PAYMENTS("selectedBillingPayments", "Поступление платежей"),
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
