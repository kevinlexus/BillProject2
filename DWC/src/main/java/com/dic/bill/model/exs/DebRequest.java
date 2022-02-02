package com.dic.bill.model.exs;

import com.dic.bill.model.bs.AddrTp;
import com.dic.bill.model.bs.Lst2;
import com.dic.bill.model.scott.House;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.sec.User;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.DynamicUpdate;

import javax.annotation.Generated;
import javax.persistence.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;


/**
 * Запросы о задолженности от УСЗН
 *
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "DEBT_REQUEST", schema="EXS")
@DynamicUpdate
@Getter @Setter
public class DebRequest implements java.io.Serializable  {

	public DebRequest() {
	}

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "DEBT_REQUEST")
	@SequenceGenerator(name="SEQ_DEBT_REQUEST", sequenceName="EXS.SEQ_DEBT_REQUEST", allocationSize=1)
    @Column(name = "id", unique=true, updatable = false, nullable = false)
	private Integer id;

	// GUID дома
	@Column(name = "HOUSE_GUID")
	private String houseGuid;

	// идентификатор запроса, присвоенный в ГИС ЖКХ
	@Column(name = "REQUEST_GUID")
	private String requestGuid;

	// номер запроса, присвоенный в ГИС ЖКХ
	@Column(name = "REQUEST_NUMBER")
	private String requestNumber;

	// адрес
	@Column(name = "ADDRESS")
	private String address;

	@Override
	public boolean equals(Object o) {
	    if (this == o) return true;
	    if (!(o instanceof DebRequest))
	        return false;

	    DebRequest other = (DebRequest)o;

	    // equivalence by id
	    return getId().equals(other.getId());
	}

	@Override
	public int hashCode() {
	    if (getId() != null) {
	        return getId().hashCode();
	    } else {
	        return super.hashCode();
	    }
	}
}

