package com.dic.bill.dto;

import lombok.Value;

import java.util.List;

@Value
public class ChangesParam {
    Integer klskId; // фин.лиц.счет
    String lsk; // лиц.счет
    List<Selobj> selObjList; // выбранные объекты
    int selObjTp; //  перерасчет по: 0-фин.лиц.сч (помещение), 1-лиц.сч., 2-выбранные объекты
    String periodFrom; // начальный период
    String periodTo; // конечный период
    boolean isAddUslSvSocn; // добавлять услуги свыше соц.н.?
    boolean isAddUslKan; // добавлять услуги водоотведения?
    int processMeter; // 0-без счетчиков, 1-в т.ч. по счетчикам, 2-только по счетчикам
    int processAccount; // 0-по всем лицевым, 1-только по закрытым, 2-только по открытым
    int processStatus; //  статусы: 0 - по всем, или ID статуса жилья из спр. status
    int processLskTp; // вариант перерасчета (0-только по основным лс., 1 - только по дополнит лс., 2 - по тем и другим)
    int processTp; //  тип перерасчета, 0 - все остальные, 1 - корректировка сальдо
    boolean isProcessEmpty; //  по лиц.счетам, где никто не проживает?
    String comment; // комментарий
    List<ChangeUsl> changeUslList; // параметры перерасчета по услугам и организациям

}
