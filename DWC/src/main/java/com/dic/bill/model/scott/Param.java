package com.dic.bill.model.scott;

import javax.persistence.*;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.Type;

/**
 * Таблица параметров
 * @author lev
 *
 */
@Entity
@Table(name = "PARAMS", schema="SCOTT")
@Immutable
@Cacheable // note как быть при переходе месяца, если закэшировано?
@org.hibernate.annotations.Cache(region = "BillDirectNeverClearCache", usage = CacheConcurrencyStrategy.READ_ONLY)
@Getter @Setter
public class Param {

	public Param() {
	}

    @Id
	@Column(name = "ID", updatable = false, nullable = false)
	private Integer id;

	// период
	@Column(name = "PERIOD", nullable = false)
	private String period;

	// считать ли услуги по дням, или укрупнёнными периодами до 15 числа и после?
	@Type(type = "org.hibernate.type.NumericBooleanType")
	@Column(name = "IS_DET_CHRG", nullable = false)
	private Boolean isDetChrg;

}

