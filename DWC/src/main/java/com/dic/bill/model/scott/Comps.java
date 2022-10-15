package com.dic.bill.model.scott;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Immutable;

import javax.persistence.*;

/**
 * Источник платежа (компьютер, банк)
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "C_COMPS", schema = "SCOTT")
@Immutable
@Cacheable
@org.hibernate.annotations.Cache(region = "BillDirectNeverClearCache", usage = CacheConcurrencyStrategy.READ_ONLY)
@Getter
@Setter
public class Comps implements java.io.Serializable {

    @Id
    @Column(name = "NKOM", updatable = false, nullable = false)
    private Integer id;

    // CD
    @Column(name = "CD", updatable = false, nullable = false)
    private String cd;

    // Организация
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_ORG", referencedColumnName = "ID")
    private Org org;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof Org)) return false;

        Comps other = (Comps) o;

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

