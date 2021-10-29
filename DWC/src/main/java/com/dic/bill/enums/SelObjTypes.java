package com.dic.bill.enums;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;


@Getter
public enum SelObjTypes {
    HOUSE("Дом", 0),
    FIN_ACCOUNT("Фин.лиц.счет", 1),
    LSK("Лиц.счет", 2),
    ALL("Весь фонд", 3);

    private String name;
    private Integer id;

    SelObjTypes(String name, Integer id) {
        this.name = name;
        this.id = id;
    }

    public static SelObjTypes getByName(String name) {
        List<SelObjTypes> list = Arrays.asList(SelObjTypes.values());
        for (SelObjTypes status : list) {
            if (status.getName().equals(name))
                return status;
        }
        return null;
    }
}
