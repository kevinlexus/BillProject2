package com.dic.app.enums;

public enum BaseForDistPays {
    IN_SAL_0(0),
    CURR_CHARGE_1(1),
    BACK_PERIOD_CHARGE_3(3),
    ALREADY_DISTRIB_PAY_4(4),
    CURR_PERIOD_CHARGE_5(5), // чем отличается от CURR_CHARGE хз
    IN_SAL_PEN_6(6),
    CURR_PERIOD_CHARGE_MOIFY_7(7), // чем отличается от CURR_CHARGE хз
    SELECTED_PERIOD_CHARGE_8(8);

    BaseForDistPays(int id) {
    }
}
