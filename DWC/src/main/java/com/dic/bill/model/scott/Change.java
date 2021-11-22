package com.dic.bill.model.scott;

import com.dic.bill.enums.ChangeTps;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;

/**
 * Перерасчет
 * @author lev
 * @version 1.01
 *
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "C_CHANGE", schema="SCOTT")
//@Table(name = "C_CHANGE", schema="LOADER1")
@Getter @Setter
public class Change implements java.io.Serializable  {

	public Change() {
	}

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ_CHANGE_ID")
	@SequenceGenerator(name = "SEQ_CHANGE_ID", sequenceName = "SCOTT.CHANGES_ID", allocationSize = 1)
	@Column(name = "id", unique = true, updatable = false, nullable = false)
	private Integer id;

	@Column(name = "LSK")
	private String lsk;

	// лиц.счет
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name="LSK", referencedColumnName="LSK", insertable = false, updatable = false)
	private Kart kart;

	// документ перерасчета
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name="DOC_ID", referencedColumnName="ID")
	private ChangeDoc changeDoc;

	// сумма
	@Column(name = "SUMMA")
	private BigDecimal summa;

	// процент перерасчета
	@Column(name = "PROC")
	private BigDecimal proc;

	// кол-во дней перерасчета
	@Column(name = "CNT_DAYS")
	private Integer cntDays;


	// период за который перерасчет
	@Column(name = "MGCHANGE")
	private String mgchange;

	@Column(name = "usl")
	private String uslId;

	// услуга
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "USL", referencedColumnName = "USl", nullable = false, insertable = false, updatable = false)
	private Usl usl;

	@Column(name = "ORG")
	private Integer orgId;

	// организация - поставщик услуги
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "ORG", referencedColumnName = "ID", nullable = false, insertable = false, updatable = false)
	private Org org;


	@Column(name = "USER_ID")
	private Integer userId;

	// тип перерасчета (0- в %, 1-в денежном выражении)
	@Column(name = "TP")
	private ChangeTps tp;

	// пользователь
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "USER_ID", referencedColumnName = "ID", nullable = false, insertable = false, updatable = false)
	private Tuser user;

	// период, которым надо провести разовые изменения
	// (сделано, чтобы можно было проводить доначисление за прошлый период, не трогая расчёт пени)
	@Column(name = "MG2")
	private String mg2;

	// Дата перерасчета
	@Column(name = "DTEK")
	private Date dt;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Change change = (Change) o;
		return Objects.equals(getId(), change.getId());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getId());
	}
}

