package com.dic.app.telegram.bot.service.menu;

public enum MeterValSaveState {
    SUCCESSFUL("Показания успешно переданы"),
    VAL_SAME_OR_LOWER("Показания те же или меньше текущих"),
    METER_NOT_FOUND("Счетчик не найден"),
    VAL_TOO_HIGH("Показания слишком большие "),
    VAL_OUT_OF_RANGE("Показания вне диапазона "), // когда сняли, меньше чем было показание на начало периода
    ERROR_WHILE_SENDING("Ошибка при передаче показаний"),
    WRONG_FORMAT("Некорректный формат показаний, используйте например: 1234.543"),
    VAL_TOO_BIG_OR_LOW("Показания вне допустимого диапазона"),
    INTERNAL_ERROR("Ошибка в Oracle package"),
    RESTRICTED_BY_DAY_OF_MONTH("В связи с формированием отчетности за месяц, " +
            "ограничена возможность отправки расхода по счетчикам, в последние дни месяца");

    private final String name;

    MeterValSaveState(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }


}
