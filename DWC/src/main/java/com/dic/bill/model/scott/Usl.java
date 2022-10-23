package com.dic.bill.model.scott;

import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;

/**
 * Справочник услуг
 *
 * @author lev
 * @version 1.01
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "USL", schema = "SCOTT")
@Immutable
@Cacheable
@org.hibernate.annotations.Cache(region = "BillDirectEntitiesCache", usage = CacheConcurrencyStrategy.READ_ONLY)
@Getter
@Setter
public class Usl implements java.io.Serializable {

    public Usl() {
    }

    @Id
    @Column(name = "USL", unique = true, updatable = false, nullable = false)
    private String id;

    // CD
    @Column(name = "CD")
    private String cd;

    // наименование
    @Column(name = "NM")
    private String name;

    // укороченное наименование
    @Column(name = "NM2")
    private String nm2;

    // упрощенное наименование, для telegram bot
    @Column(name = "NM_FOR_BOT")
    private String nameForBot;

    // короткое название услуги (для удобного представления счета при оплате)
    @Column(name = "NM_SHORT")
    private String nameShort;

    // наименование поля счетчик в scott.kart
    @Column(name = "COUNTER")
    private String counter;

    // ед.измерения
    @Column(name = "ED_IZM")
    private String unitVol;

    // сортировка
    @Column(name = "NPP")
    private Integer npp;

    // для справочника дат начала обязательств по долгу -  PEN_DT Тип услуги (0-прочие услуги, 1-капремонт)
    @Column(name = "TP_PEN_DT")
    private Integer tpPenDt;

    // для справочника ставок рефинансирования - PEN_REF Тип услуги (0-прочие услуги, 1-капремонт)
    @Column(name = "TP_PEN_REF")
    private Integer tpPenRef;

    // 0 - коэфф. в справочнике тарифов, 1 - норматив в справочнике тарифов,
    // 2 - koeff-коэфф и norm-норматив, 3-koeff и norm-оба коэфф
    // 4 - koeff-коэфф и norm-норматив, только если koeff == 0, то берётся в объемы, и не берётся в c_charge (для х.в., г.в. и водоотведения)
    @Column(name = "SPTARN")
    private Integer sptarn;

    // родительская услуга (например Х.В. 0 прожив --> Х.В.)
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "PARENT_USL", referencedColumnName = "USl")
    private Usl parentUsl;

    // услуга без проживающих
    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinColumn(name = "USL_EMPT", referencedColumnName = "USL")
    private Usl uslEmpt;

    // услуга свыше соцнормы
    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinColumn(name = "USL_P", referencedColumnName = "USL")
    private Usl uslOverSoc;

    // услуга для определения объема (иногда делается подстановка)
    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinColumn(name = "USL_VOL", referencedColumnName = "USL")
    private Usl uslVol;

    // тип услуги 2 -- 0 - услуга коммунальная (х.в.), 1 - услуга жилищная (тек.сод.)
    @Column(name = "USL_TYPE2")
    private Integer type2;

    // вариант расчета услуги
    @Column(name = "FK_CALC_TP")
    private Integer fkCalcTp;

    // цены по услуге
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.READ_ONLY)
    @OneToMany(mappedBy = "usl", fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
    private Collection<Price> price = new HashSet<>(0);

    // порядок расчета услуг
    @Column(name = "USL_ORDER")
    private Integer uslOrder;

    // дочерняя услуга (связанная) например х.в.--> х.в.МОП.
    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinColumn(name = "FK_USL_CHLD", referencedColumnName = "USL")
    private Usl uslChild;

    // коды REU, для округления, для ГИС ЖКХ
/*
    @OneToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "USL", referencedColumnName = "USL")
    private Set<UslRound> uslRound = new HashSet<>(0);
*/

    // использовать ли объем, в расчете водоотведения, например для двухкомпонентной услуги (х.в. для г.в.+ тепл.энерг для г.в.)
    @Type(type = "org.hibernate.type.NumericBooleanType")
    @Column(name = "USE_VOL_CAN", updatable = false)
    private Boolean isUseVolCan;

    // cкрыть начисление (используется для получения расценки по вирт.услуг в счетах)
    @Type(type = "org.hibernate.type.NumericBooleanType")
    @Column(name = "HIDE_CHRG", updatable = false)
    private Boolean isHideChrg;

    // максимальный объем за одну итерацию передачи объемов через ЛК или Telegram bot
    @Column(name = "MAX_VOL")
    private BigDecimal maxVol;

    /**
     * Получить фактическую услугу, поставляющую объем (иногда нужно, например для услуги fkCalcTp=31)
     */
    @Transient
    public Usl getMeterUslVol() {
        return getUslVol() != null ? getUslVol() : this;
    }

    /**
     * Является ли услуга жилищной?
     */
    @Transient
    public boolean isHousing() {
        return getType2() != null && getType2().equals(1);
    }

    /**
     * Базовый состав услуг х.в, г.в.
     */
    @Transient
    public boolean isBaseWaterCalc() {
        return Utl.in(getFkCalcTp(), 17, 18, 55, 56);
    }

    /**
     * Базовый состав услуг х.в, г.в. - 2
     */
    @Transient
    public boolean isBaseWaterCalc2() {
        return Utl.in(getFkCalcTp(), 17, 18, 31, 52);
    }

    /**
     * Расчет по х.в. г.в., водоотв. и подобным услугам? или calc_tp in (17, 18, 19, 55, 56, 57)
     */
    @Transient
    public boolean isKindOfWaterCalc() {
        return isBaseWaterCalc() || Utl.in(getFkCalcTp(), 19, 57);
    }

    /**
     * Расчет по х.в. г.в., водоотв. и подобным услугам, расширенный состав ? или calc_tp in (17, 18, 19, 52, 53, 55, 56, 57)
     */
    @Transient
    public boolean isCounterCalcExt() {
        return isKindOfWaterCalc() || Utl.in(getFkCalcTp(), 52, 53);
    }


    /**
     * Расчет по услугам, содержащим счетчики? или calc_tp in (14, 17, 18, 31, 52, 53, 54, 55, 56)
     */
    @Transient
    public boolean isCounterCalc() {
        return isBaseWaterCalc() || Utl.in(getFkCalcTp(), 14, 31, 52, 53, 54);
    }

    /**
     * Услуги для расчета объемов по ОДПУ
     */
    @Transient
    public boolean isKindOfODPU() {
        return Utl.in(getFkCalcTp(), 3, 4, 17, 18, 31, 38, 40);
    }

    /**
     * Услуги для расчета объемов по ОДПУ, по воде
     */
    @Transient
    public boolean isKindOfWaterAndODPU() {
        return Utl.in(getFkCalcTp(), 3, 4, 17, 18, 38, 40);
    }

    /**
     * Получить фактическую услугу свыше соц.нормы
     */
    @Transient
    public Usl getFactUslOverSoc() {
        if (getUslOverSoc() != null) {
            return getUslOverSoc();
        } else {
            // пустая услуга свыше соц.нормы, вернуть основную
            return this;
        }
    }

    /**
     * Получить фактическую услугу 0 зарег
     */
    @Transient
    public Usl getFactUslEmpt() {
        if (getUslEmpt() != null) {
            return getUslEmpt();
        } else {
            // пустая услуга 0 зарег, вернуть свыше соц.нормы
            if (getUslOverSoc() != null) {
                return getUslOverSoc();
            } else {
                // пустая услуга свыше соц.нормы, вернуть основную
                return this;
            }
        }
    }

    /**
     * Является ли услуга основной? (заполнено fk_calc_tp)
     */
    @Transient
    public boolean isMain() {
        return getFkCalcTp() != null;
    }

    /**
     * Рассчитывается ли услуга по площади? (для округления площади в рассчете)
     */
    @Transient
    public boolean isCalcByArea() {
        return Utl.in(fkCalcTp, 25) // текущее содержание и подобные услуги (без свыше соц.нормы и без 0 проживающих)
                || fkCalcTp.equals(7) // найм (только по муниципальным помещениям) расчет на м2
                || fkCalcTp.equals(24) || fkCalcTp.equals(32) // прочие услуги, расчитываемые как расценка * норматив * общ.площадь
                || fkCalcTp.equals(36)// вывоз жидких нечистот и т.п. услуги
                || fkCalcTp.equals(37); // капремонт
    }

    /**
     * Особый расчет цены
     */
    @Transient
    public boolean isCalcPriceSpecial() {
        return fkCalcTp.equals(7) // найм (только по муниципальным помещениям) расчет на м2
                || fkCalcTp.equals(25) // текущее содержание и подобные услуги (без свыше соц.нормы и без 0 проживающих)
                || fkCalcTp.equals(32) // прочие услуги, расчитываемые как расценка * норматив * общ.площадь
                || fkCalcTp.equals(36)// вывоз жидких нечистот и т.п. услуги
                || fkCalcTp.equals(37); // капремонт
    }

    /**
     * Учитывать в C_CHARGE, даже если нет объема - пока не удалять, принимаем решение с Кис до 01.06.19
     */
/*	@Transient
	public boolean isTakeForChargeWhenEmptyVol()
	{
		return fkCalcTp.equals(11111)
	}*/
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof Usl))
            return false;

        Usl other = (Usl) o;

        if (id == other.getId()) return true;
        if (id == null) return false;

        // equivalence by id
        return id.equals(other.getId());
    }

    @Override
    public int hashCode() {
        if (id != null) {
            return id.hashCode();
        } else {
            return super.hashCode();
        }
    }

}

