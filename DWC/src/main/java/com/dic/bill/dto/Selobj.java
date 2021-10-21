package com.dic.bill.dto;

import lombok.Value;

@Value
public class Selobj {
    Integer id; // Id из Direct
    String kul; // код ул.
    String nd;  // № дома
    Integer klskId; // фин.лиц.сч.
    Integer tp; // тип объекта, 0 - дом, 1- фин.лиц.
}
