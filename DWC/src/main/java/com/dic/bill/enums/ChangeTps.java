package com.dic.bill.enums;

import lombok.Getter;


@Getter
public enum ChangeTps {
    PROC(0),
    ABS(1);

    private Integer id;

    ChangeTps(Integer id) {
        this.id = id;
    }

}
