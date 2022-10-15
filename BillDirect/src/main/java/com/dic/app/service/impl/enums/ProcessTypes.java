package com.dic.app.service.impl.enums;

import lombok.Getter;

import java.util.Arrays;

public enum ProcessTypes {
    CHARGE(0), // 0-начисление
    DEBT_PEN(1),  // 1 - задолженность и пеня
    DIST_VOL(2), // 2 - распределение объемов по вводу
    CHARGE_FOR_DIST(3), // 3 - начисление для распределения по вводу
    CHARGE_SINGLE_USL(4), // 4 - начисление по одной услуге
    MIGRATION(5); // 5 - миграция долгов
    @Getter
    private final int id;

    ProcessTypes(int id) {
        this.id = id;
    }

    public static ProcessTypes getById(int id) {
        return Arrays.stream(ProcessTypes.values()).filter(t -> t.id==id)
                .findFirst().orElse(null);
    }

}
