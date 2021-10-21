package com.dic.bill.model.scott;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * АРХИВ. Лицевой счет
 */
@Entity
@Table(name = "ARCH_KART", schema = "SCOTT")
@Getter
@Setter
@NoArgsConstructor
public class Akart implements java.io.Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ_ARCH_KART")
    @SequenceGenerator(name = "SEQ_ARCH_KART", sequenceName = "SCOTT.ARCH_KART_ID", allocationSize = 1)
    @Column(name = "id", unique = true, updatable = false, nullable = false)
    private Integer id;

    // лиц.счет
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name="LSK", referencedColumnName="LSK")
    private Kart kart;

    // период
    @Column(name = "MG")
    private String mg;

    // признак счета
    @Column(name = "PSCH", nullable = false)
    private Integer psch;

    // наличие эл.эн счетчика (0, null - нет, 1 - есть)
    @Column(name = "SCH_EL")
    private Integer schEl;

    // кол-во проживающих
    @Column(name = "KPR")
    private Integer kpr;

    // Кол-во вр.зарег.
    @Column(name = "KPR_WR")
    private Integer kprWr;

    // Кол-во вр.отсут.
    @Column(name = "KPR_OT")
    private Integer kprOt;

    // Кол-во собственников
    @Column(name = "KPR_OWN")
    private Integer kprOwn;

    // тип лиц.счета
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TP", referencedColumnName = "ID")
    private Lst tp;

    // статус лиц.счета
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "STATUS", referencedColumnName = "ID", nullable = false)
    private Status status;

    // кран из системы отопления
    @Type(type = "org.hibernate.type.NumericBooleanType")
    @Column(name = "KRAN1")
    private Boolean isKran1;

    // активный ли лицевой счет?
    @Transient
    public boolean isActual() {
        return !psch.equals(8) && !psch.equals(9);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Akart akart = (Akart) o;
        return Objects.equals(getId(), akart.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }


}

