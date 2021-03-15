package com.dic.bill.model.scott;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.Type;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * Строка из справочника spr_gen_itm,
 * описывающая шаги выполнения формирования
 *
 * @version 1.0
 */
@Entity
@Table(name = "SPR_GEN_ITM", schema = "SCOTT")
@DynamicUpdate
@Getter
@Setter
public class SprGenItm {

    @Id
    @Column(name = "ID", unique = true, nullable = false)
    private Integer id;

    @NaturalId
    @Column(name = "CD", unique = true, nullable = false)
    private String cd;

    @Column(name = "NAME")
    private String name;

    @Column(name = "STATE")
    private String state;

    @Column(name = "NPP")
    private Integer npp;

    @Column(name = "NPP2")
    private Integer npp2;

    @Column(name = "ERR")
    private Integer err;

    @Column(name = "PROC")
    private Double proc;

    @Column(name = "DURATION")
    private Integer duration;

    // Выбрано пользователем?
    @Type(type = "org.hibernate.type.NumericBooleanType")
    @Column(name = "SEL")
    private Boolean sel;

    // Отображать пользователю?
    @Type(type = "org.hibernate.type.NumericBooleanType")
    @Column(name = "V")
    private Boolean v;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Krasnoyarsk")
    @Column(name = "DT1")
    private Date dt1;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Krasnoyarsk")
    @Column(name = "DT2")
    private Date dt2;

}