package com.dic.bill.model.scott;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.math.BigDecimal;

/**
 * Оборотная ведомость
 *
 * @author lev
 * @version 1
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "XITOG3_LSK", schema = "SCOTT")
@Getter
@Setter
public class Xitog3Lsk implements java.io.Serializable {

    public Xitog3Lsk() {
    }

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ_XITOG3_LSK_ID")
    @SequenceGenerator(name = "SEQ_XITOG3_LSK_ID", sequenceName = "SCOTT.XITOG3_LSK_ID", allocationSize = 1)
    @Column(name = "id", unique = true, updatable = false, nullable = false)
    private Integer id;

    // лиц.счет
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "LSK", referencedColumnName = "LSK")
    private Kart kart;

    @Column(name = "charges", updatable = false, nullable = false)
    private BigDecimal charges; // начисление

    @Column(name = "pcur", updatable = false, nullable = false)
    private BigDecimal pcur; // текущая пеня

    @Column(name = "pinsal", updatable = false, nullable = false)
    private BigDecimal pinsal; // вх.сальдо пени

    @Column(name = "poutsal", updatable = false, nullable = false)
    private BigDecimal poutsal; // исх.сальдо пени

    @Column(name = "mg", updatable = false, nullable = false)
    private String mg; // архивный период

}

