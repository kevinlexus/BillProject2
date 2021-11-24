package com.dic.bill.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;

@NoArgsConstructor
@Getter
@Setter
public class ChangesParam {
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd.MM.yyyy")
    private Date dt; // дата перерасчета
    private String user; // пользователь
    private List<Selobj> selObjList; // выбранные объекты
    private String periodFrom; // начальный период
    private String periodTo; // конечный период
    @JsonIgnore
    private LocalDate dtFrom; // дата начала перерасчета (заполняется на бэке)
    @JsonIgnore
    private LocalDate dtTo; // дата окончания перерасчета (заполняется на бэке)
    private String periodProcess; // провести периодом
    private Boolean isAddUslSvSocn; // добавлять услуги свыше соц.н.?
    private Boolean isAddUslWaste; // добавлять услуги водоотведения?Rege
    private int processMeter; // 0-без счетчиков, 1-в т.ч. по счетчикам, 2-только по счетчикам
    private int processAccount; // 0-по всем лицевым, 1-только по закрытым, 2-только по открытым
    private int processStatus; //  статусы: 0 - по всем, или ID статуса жилья из спр. status
    private int processLskTp; // вариант перерасчета (0-только по основным лс., 1 - только по дополнит лс., 2 - по тем и другим)
    private int processKran; // 0-по всем лицевым, 1-при наличии крана из системы отоп., 2- при отсутствии крана
    private int processEmpty; // 0-по всем лицевым, 1-там где никто не проживает
    private String comment; // комментарий
    private List<ChangeUsl> changeUslList; // параметры перерасчета по услугам и организациям
    @JsonIgnore
    private String archPeriodTo; // период выборки архивов (заполняется на бэке)
    @JsonIgnore
    private List<String> uslListForQuery; // список услуг (заполняется на бэке)
}
