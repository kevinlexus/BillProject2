package com.dic.bill.model.scott;

import com.dic.bill.model.exs.Ulist;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Immutable;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.Date;

/**
 * Реестр платежей от агента
 */
@Getter @Setter
@SuppressWarnings("serial")
@Entity
@Table(name = "LOAD_BANK", schema="SCOTT")
public class LoadBank implements java.io.Serializable {

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ_LOAD_BANK")
	@SequenceGenerator(name = "SEQ_LOAD_BANK", sequenceName = "SCOTT.LOAD_BANK_ID", allocationSize = 1)
	@Column(name = "id", unique = true, updatable = false, nullable = false)
	private Integer id;

	@Column(name = "DTEK")
	private Date dtek;

    @Column(name = "LSK")
	private String lsk;

    @Column(name = "CODE")
	private String code; // код банковской операции 01-оплата, 02-пеня (возможно устаревшее) используется в c_get_pay.recv_payment_bank

	@Column(name = "SUMMA")
	private BigDecimal summa;

	@Column(name = "DOPL")
	private String dopl;

	@Column(name = "NKOM")
	private String nkom;

	@Override
	public boolean equals(Object o) {
	    if (this == o) return true;
	    if (o == null || !(o instanceof LoadBank))
	        return false;

	    LoadBank other = (LoadBank)o;

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

