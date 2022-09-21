package com.dic.app.telegram.bot.service.menu;

public enum Menu {
    ROOT,
    UNDEFINED,
    MAIN,
    SELECT_ADDRESS,
    SELECT_METER,
    INPUT_VOL,
    SELECT_REPORT,
    SELECT_FLOW,
    SELECT_CHARGE,
    INPUT_WRONG;

    public static Menu getByNameIgnoreCase(String name) {
        Menu[] list = Menu.values();
        for (Menu elem : list) {
            if (elem.toString().compareToIgnoreCase(name) == 0)
                return elem;
        }
        return null;

    }
}
