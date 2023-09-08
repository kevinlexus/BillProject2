package com.dic.bill.model.scott;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.Type;

import javax.persistence.*;

/**
 * Организация
 *
 *
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "T_ORG", schema="SCOTT")
@Immutable
@Cacheable
@org.hibernate.annotations.Cache(region = "BillDirectEntitiesCache", usage = CacheConcurrencyStrategy.READ_ONLY)
@Getter @Setter
public class Org implements java.io.Serializable {

	@Id
    @Column(name = "ID", updatable = false, nullable = false)
	private Integer id;

	// CD
	@Column(name = "CD", updatable = false, nullable = false)
	private String cd;

	// код REU
    @Column(name = "REU")
	private String reu;

	// код TREST
    @Column(name = "TREST")
	private String trest;

	// Наименование
    @Column(name = "NAME")
	private String name;

	// ИНН
    @Column(name = "INN")
	private String inn;

	// БИК
    @Column(name = "BIK")
	private String bik;

	// ОГРН
	@Column(name = "KOD_OGRN")
	private String ogrn;

	// расчетный счет
    @Column(name = "RASCHET_SCHET")
	private String rSchet;

	// расчетный счет - 2
    @Column(name = "RASCHET_SCHET2")
	private String rSchet2;

	// расчетный счет для гис ЖКХ
	@Column(name = "R_SCH_GIS")
	private String operAccGis;

	// тип организации
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name="FK_ORGTP", referencedColumnName="ID")
	private OrgTp orgTp;

	// тип распределения оплаты KWTP_MG (0-общий тип, 1 - сложный тип (ук 14,15 Кис)
	@Column(name = "DIST_PAY_TP")
	private Integer distPayTp;

	// осуществлять обмен по организации с ГИС ЖКХ? (0-нет, 1-да)
	@Type(type= "org.hibernate.type.NumericBooleanType")
	@Column(name = "IS_EXCHANGE_GIS", updatable = false)
	private Boolean isExchangeGis;

	// тип организации для ГИС ЖКХ (1-УО (упр.орг.), 2-РСО, 3-ТКО)
	@Column(name = "ORG_TP_GIS")
	private Integer orgTpGis;

	// группировка для долгов Сбера (Не заполнено - брать REU, заполнено - группировать по этому полю)
	@Column(name = "GRP_DEB")
	private Integer grpDeb;

	// обмен внешними лиц счетами (0-нет, 1-да) (Например Полыс - Чистый город)
	@Type(type= "org.hibernate.type.NumericBooleanType")
	@Column(name = "IS_EXCHANGE_EXT", updatable = false)
	private Boolean isExchangeExt;

	// при загрузке реестра внешних лиц счетов, создавать ли лиц.счета в Kart (Например Кис - ФКР) // note Не используется?? ред.12.08.21
	//@Type(type= "org.hibernate.type.NumericBooleanType")
	//@Column(name = "IS_CREATE_EXT_LSK_IN_KART", updatable = false)
	//private Boolean isCreateExtLskInKart;

	// услуга для создания внешних лиц счетов (работает в случае isCretateExtLskInKart==true)
	@OneToOne(fetch = FetchType.LAZY, cascade=CascadeType.ALL)
	@JoinColumn(name="USL_FOR_CREATE_EXT_LSK", referencedColumnName="USL", updatable = false) // note убрать после merge с доработаким по пене - updatable = false - чтобы не было Update Foreign key
	private Usl uslForCreateExtLskKart;

	// формат загрузочного файла внешних лиц.счетов (обычно: 0-ЧГК, 1-ФКП)
	@Column(name = "EXT_LSK_FORMAT_TP")
	private Integer extLskFormatTp;

	// Формат файла платежей, для выгрузки (обычно: 0-ЧГК,1-ФКП)
	@Column(name = "EXT_LSK_PAY_FORMAT_TP")
	private Integer extLskPayFormatTp;

	// загружать сальдо вн.лиц.счетов, (0-как на начало месяца (ФКР Кис), 1-как за прошлый период (ФКР Полыс.))
	@Column(name = "EXT_LSK_LOAD_SALDO_TP")
	private Integer extLskLoadSaldoTp;

	// загружать оплату по вн.лиц.счетам?
	@Type(type= "org.hibernate.type.NumericBooleanType")
	@Column(name = "EXT_LSK_LOAD_PAY", updatable = false)
	private Boolean isExtLskLoadPay;

	// вариант заполнения задолженности и пени в ПД (0-обычный, 1-ГисЖкхЭкоТек (Кис.))
	@Column(name = "VAR_DEB_PEN_PD_GIS")
	private Integer varDebPenPdGis;

	// отправлять ответы на запросы о задолженности в УСЗН автоматически?
	@Type(type= "org.hibernate.type.NumericBooleanType")
	@Column(name = "IS_AUTO_SEND_DEB_REQ", updatable = false)
	private Boolean isAutoSendDebReq;

	@Transient
	public boolean isUO() {
		if (orgTpGis == null) {
			return false;
		} else {
			return orgTpGis.equals(1);
		}
	}

	@Transient
	public boolean isRSO() {
		if (orgTpGis == null) {
			return false;
		} else {
			return orgTpGis.equals(2);
		}
	}

	@Transient
	public boolean isTKO() {
		if (orgTpGis == null) {
			return false;
		} else {
			return orgTpGis.equals(3);
		}
	}

	@Override
	public boolean equals(Object o) {
	    if (this == o) return true;
	    if (o == null || !(o instanceof Org))
	        return false;

	    Org other = (Org)o;

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

