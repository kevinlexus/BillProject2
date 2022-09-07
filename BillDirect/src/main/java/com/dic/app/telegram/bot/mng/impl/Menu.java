package com.dic.app.telegram.bot.mng.impl;

enum Menu {
    UNDEFINED("Неопределено"),
    MAIN("Основное меню"),
    SELECT_ADDRESS("Выбор адреса"),
    SELECT_METER("Выбор счетчика"),
    INPUT_VOL("Введите показания"),
    SELECT_BILLING("Движение средств"),
    INPUT_WRONG("Некорректный ввод");

    private final String name;

    Menu(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
