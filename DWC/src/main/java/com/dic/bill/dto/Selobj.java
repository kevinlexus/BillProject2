package com.dic.bill.dto;

import com.dic.bill.enums.SelObjTypes;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class Selobj {
    Integer id; // Id из Direct
    String kul; // код ул.
    String nd;  // № дома
    Long klskId; // фин.лиц.сч.
    String lsk; // лиц.счет
    SelObjTypes tp; // тип объекта, 0-дом, 1-фин.лиц., 2-лиц.сч., 3-весь фонд
}
