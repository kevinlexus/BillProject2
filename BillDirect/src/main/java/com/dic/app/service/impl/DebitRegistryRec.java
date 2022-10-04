package com.dic.app.service.impl;

import lombok.Getter;
import lombok.Setter;

/**
 * Запись реестра задолженности для отправки в банк
 *
 * @version 1.00
 */
@Getter
@Setter
public class DebitRegistryRec {

    // элемент реестра
    String elem;
    // текущий разделитель
    String delimeter = ";";
    // результирующая строка
    StringBuilder result = new StringBuilder();

    /**
     * обрезать, добавить элемент
     */
    public void addElem(String... elem) {
        for (String singleElem : elem) {
            result.append(singleElem != null ? singleElem.trim() : "").append(getDelimeter());
        }
    }

    public void addElem(String elem, String delimiter) {
        result.append(elem != null ? elem.trim() : "").append(delimiter);
    }

    /**
     * Инициализировать объект
     */
    public void init() {
        result = new StringBuilder();
    }
}
