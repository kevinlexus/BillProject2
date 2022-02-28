package com.dic.bill.model.scott;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;

import javax.persistence.*;

/**
 * Элемент списка
 */
@Getter
@Setter
@SuppressWarnings("serial")
@Entity
@Cacheable
@org.hibernate.annotations.Cache(region = "BillDirectEntitiesCacheUserPerm", usage = CacheConcurrencyStrategy.READ_WRITE)
@Table(name = "C_USERS_PERM", schema = "SCOTT")
public class UserPerm implements java.io.Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "ID", updatable = false, nullable = false)
    private Integer id;

    // УК
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_REU", referencedColumnName = "REU", insertable = false, updatable = false)
    private Org uk;

    // пользователь
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "USER_ID", referencedColumnName = "ID", nullable = false, insertable = false, updatable = false)
    private Tuser user;

    // тип доступа
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_PERM_TP", referencedColumnName = "ID", nullable = false, insertable = false, updatable = false)
    private Lst tp;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof UserPerm))
            return false;

        UserPerm other = (UserPerm) o;

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

