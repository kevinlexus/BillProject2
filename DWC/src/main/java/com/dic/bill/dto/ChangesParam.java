package com.dic.bill.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@NoArgsConstructor
@Getter
@Setter
public class ChangesParam {
    List<Selobj> selObjList; // выбранные объекты
    String periodFrom; // начальный период
    String periodTo; // конечный период
    String periodProcess; // провести периодом
    Boolean isAddUslSvSocn; // добавлять услуги свыше соц.н.?
    Boolean isAddUslWaste; // добавлять услуги водоотведения?
    int processMeter; // 0-без счетчиков, 1-в т.ч. по счетчикам, 2-только по счетчикам
    int processAccount; // 0-по всем лицевым, 1-только по закрытым, 2-только по открытым
    int processStatus; //  статусы: 0 - по всем, или ID статуса жилья из спр. status
    int processLskTp; // вариант перерасчета (0-только по основным лс., 1 - только по дополнит лс., 2 - по тем и другим)
    int processTp; //  тип перерасчета, 0 - все остальные, 1 - корректировка сальдо
    int processKran; // 0-по всем лицевым, 1-при наличии крана из системы отоп., 2- при отсутствии крана
    int processEmpty; // 0-по всем лицевым, 1-там где никто не проживает
    String comment; // комментарий
    List<ChangeUsl> changeUslList; // параметры перерасчета по услугам и организациям

}
