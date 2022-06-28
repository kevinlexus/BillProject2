package com.dic.bill.model.exs;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Справочник элементов
 * @author lev
 *
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "U_LIST", schema="EXS")
@Getter@Setter
@Cacheable // данная сущность не содержит триггеров evict кэша, поэтому её нельзя обновлять в БД через SQL (она может быть обновлена из Java, при загрузке справочников)
@org.hibernate.annotations.Cache(region = "BillDirectEntitiesCacheReadWrite", usage = CacheConcurrencyStrategy.READ_WRITE)
public class Ulist implements java.io.Serializable  {

	public Ulist() {
	}

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ_ULIST")
	@SequenceGenerator(name="SEQ_ULIST", sequenceName="EXS.SEQ_BASE", allocationSize=1)
    @Column(name = "id", unique=true, updatable = false, nullable = false)
	private Integer id;

	public Ulist(String name, String guid, Date dt1, Date dt2,
			Boolean actual, UlistTp ulistTp, Integer npp, String value, Ulist parent,
			String refCode, String refGuid, String valTp) {
		super();
		this.name = name;
		this.guid = guid;
		this.dt1 = dt1;
		this.dt2 = dt2;
		this.actual = actual;
		this.ulistTp = ulistTp;
		this.npp = npp;
		this.s1 = value;
		this.parent = parent;
		this.refCode = refCode;
		this.refGuid = refGuid;
		this.valTp = valTp;
	}

	// наименование элемента
	@Column(name = "NAME")
	private String name;

	// значение элемента
	@Column(name = "S1")
	private String s1;

	// ИЗ ГИС ЖКХ: GUID элемента
	@Column(name = "GUID")
	private String guid;

	// ИЗ ГИС ЖКХ: Дата начала действия значения
	@Column(name = "DT1")
	private Date dt1;

	// ИЗ ГИС ЖКХ: Дата окончания действия значения
	@Column(name = "DT2")
	private Date dt2;

	// ИЗ ГИС ЖКХ: Признак актуальности элемента справочника
	@Type(type= "org.hibernate.type.NumericBooleanType")
	@Column(name = "ACTUAL")
	private Boolean actual;

	// тип справочника
	@ManyToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
	@JoinColumn(name="FK_LISTTP", referencedColumnName="ID", nullable = false)
	private UlistTp ulistTp;

	// номер порядковый
	@Column(name = "NPP", nullable = false)
	private Integer npp;

	// ref code
	@Column(name = "REF_CODE")
	private String refCode;

	// ref GUID
	@Column(name = "REF_GUID")
	private String refGuid;

	// ИЗ ГИС ЖКХ: [(NM)number;  (ST)string;  (DT)date;  (BL) boolean (RF) reference (OK) OkeiRefFieldType]
	@Column(name = "VAL_TP")
	private String valTp;

	// родительский элемент
	@OneToOne(fetch = FetchType.LAZY)
	@JoinColumn(name="PARENT_ID", referencedColumnName="ID") // ред. 21.05.2019 - закомментировал nullable = true - выдавало: ibernate.PropertyValueException: not-null property references a null or transient value : com.dic.bill.model.exs.Ulist.parent
	private Ulist parent;

	// дочерние элементы
	@OneToMany(fetch = FetchType.LAZY)
	@JoinColumn(name="PARENT_ID", referencedColumnName="ID", insertable = false, updatable = false, nullable = false)
	private List<Ulist> child = new ArrayList<>(0);

	// связь записи услуги ОИ с основной услугой
	@OneToOne(fetch = FetchType.LAZY)
	@JoinColumn(name="PARENT_ID2", referencedColumnName="ID")
	private Ulist parent2;

	// связь услуги повыш. коэфф с основной услугой
	@OneToOne(fetch = FetchType.LAZY)
	@JoinColumn(name="PARENT_ID3", referencedColumnName="ID")
	private Ulist parent3;

	// не присоединять услугу к ПД непосредственно (обычно для Повыш.коэфф.)
	@Type(type= "org.hibernate.type.NumericBooleanType")
	@Column(name = "HIDE_IN_PD")
	private Boolean isHideInPd;

	public static final class UlistBuilder {
		private Integer id;
		// CD элемента
        private String cd;
		// наименование элемента
        private String name;
		// значение элемента
        private String s1;
		// ИЗ ГИС ЖКХ: GUID элемента
        private String guid;
		// ИЗ ГИС ЖКХ: Дата начала действия значения
        private Date dt1;
		// ИЗ ГИС ЖКХ: Дата окончания действия значения
        private Date dt2;
		// ИЗ ГИС ЖКХ: Признак актуальности элемента справочника
        private Boolean actual;
		// тип справочника
        private UlistTp ulistTp;
		// номер порядковый
        private Integer npp;
		// ref code
        private String refCode;
		// ref GUID
        private String refGuid;
		// ИЗ ГИС ЖКХ: [(NM)number;  (ST)string;  (DT)date;  (BL) boolean (RF) reference (OK) OkeiRefFieldType]
        private String valTp;
		// родительский элемент
        private Ulist parent;
		// дочерние элементы
        private List<Ulist> child = new ArrayList<Ulist>(0);
		// Связь записи услуги ОИ с основной услугой
        private Ulist parent2;

		private UlistBuilder() {
		}

		public static UlistBuilder anUlist() {
			return new UlistBuilder();
		}

		public UlistBuilder withId(Integer id) {
			this.id = id;
			return this;
		}

		public UlistBuilder withCd(String cd) {
			this.cd = cd;
			return this;
		}

		public UlistBuilder withName(String name) {
			this.name = name;
			return this;
		}

		public UlistBuilder withS1(String s1) {
			this.s1 = s1;
			return this;
		}

		public UlistBuilder withGuid(String guid) {
			this.guid = guid;
			return this;
		}

		public UlistBuilder withDt1(Date dt1) {
			this.dt1 = dt1;
			return this;
		}

		public UlistBuilder withDt2(Date dt2) {
			this.dt2 = dt2;
			return this;
		}

		public UlistBuilder withActual(Boolean actual) {
			this.actual = actual;
			return this;
		}

		public UlistBuilder withUlistTp(UlistTp ulistTp) {
			this.ulistTp = ulistTp;
			return this;
		}

		public UlistBuilder withNpp(Integer npp) {
			this.npp = npp;
			return this;
		}

		public UlistBuilder withRefCode(String refCode) {
			this.refCode = refCode;
			return this;
		}

		public UlistBuilder withRefGuid(String refGuid) {
			this.refGuid = refGuid;
			return this;
		}

		public UlistBuilder withValTp(String valTp) {
			this.valTp = valTp;
			return this;
		}

		public UlistBuilder withParent(Ulist parent) {
			this.parent = parent;
			return this;
		}

		public UlistBuilder withChild(List<Ulist> child) {
			this.child = child;
			return this;
		}

		public UlistBuilder withParent2(Ulist parent2) {
			this.parent2 = parent2;
			return this;
		}

		public Ulist build() {
			Ulist ulist = new Ulist();
			ulist.setId(id);
			ulist.setName(name);
			ulist.setS1(s1);
			ulist.setGuid(guid);
			ulist.setDt1(dt1);
			ulist.setDt2(dt2);
			ulist.setActual(actual);
			ulist.setUlistTp(ulistTp);
			ulist.setNpp(npp);
			ulist.setRefCode(refCode);
			ulist.setRefGuid(refGuid);
			ulist.setValTp(valTp);
			ulist.setParent(parent);
			ulist.setChild(child);
			ulist.setParent2(parent2);
			return ulist;
		}
	}


}

