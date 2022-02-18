package com.dic.app.enums;

import lombok.Getter;

// статусы запроса по задолженности ГИС, для внутреннего использования
@Getter
public enum DebtSubRequestInnerStatuses {
    RECEIVED(0), // принято УК от ГИС
    SENT(1), // изменено и подготовлено к отправке в ГИС
    PROCESSING(2); // выполняется отправка


    private final int id;

    DebtSubRequestInnerStatuses(int id) {
        this.id=id;
    }
}
