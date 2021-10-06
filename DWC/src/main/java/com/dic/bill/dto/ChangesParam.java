package com.dic.bill.dto;

import java.util.List;

public class ChangesParam {
    String lsk;
    Integer houseId;
    List<String> periods;
    boolean isAddUslSvSocn; // добавлять услуги свыше соц.н.?
    boolean isAddUslKan; // добавлять услуги водоотведения?
    int processMeter; // 0-без счетчиков, 1-в т.ч. по счетчикам, 2-только по счетчикам
    int processAccount; // 0-по всем лицевым, 1-только по закрытым, 2-только по открытым
    int processStatus; //  статусы: 0 - по всем, или ID статуса жилья из спр. status
    int processLskTp; // вариант перерасчета (0-только по основным лс., 1 - только по дополнит лс., 2 - по тем и другим)
    int processTp; //  тип перерасчета, 0 - все остальные, 1 - корректировка сальдо
    boolean isProcessEmpty; //  по лиц.счетам, где никто не проживает?
    String comment;

}
