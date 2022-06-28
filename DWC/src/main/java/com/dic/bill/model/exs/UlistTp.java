package com.dic.bill.model.exs;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Тип справочника
 *
 * @author lev
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "U_LISTTP", schema = "EXS")
@Getter @Setter
@DynamicUpdate
@Cacheable // данная сущность не содержит триггеров evict кэша, поэтому её нельзя обновлять в БД через SQL (она может быть обновлена из Java, при загрузке справочников)
@org.hibernate.annotations.Cache(region = "BillDirectEntitiesCacheReadWrite", usage = CacheConcurrencyStrategy.READ_WRITE)
public class UlistTp implements java.io.Serializable {

    public UlistTp() {
    }

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ_ULISTTP")
    @SequenceGenerator(name = "SEQ_ULISTTP", sequenceName = "EXS.SEQ_BASE", allocationSize = 1)
    @Column(name = "id", unique = true, updatable = false, nullable = false)
    private Integer id;

    // CD элемента (ИЗ ГИС ЖКХ: С префиксом "GIS_" Реестровый номер справочника.)
    @Column(name = "CD", updatable = false)
    private String cd;

    // Наименование элемента (ИЗ ГИС ЖКХ: Наименование справочника.)
    @Column(name = "NAME")
    private String name;

    // ИЗ ГИС ЖКХ: Дата и время последнего изменения справочника.
    @Column(name = "DT1")
    private Date dt1;

    // ИЗ ГИС ЖКХ: Группа справочника: NSI - (по умолчанию) общесистемный NSIRAO - ОЖФ
    @Column(name = "GRP")
    private String grp;

    // ID элемента во внешней системе (ИЗ ГИС ЖКХ: Реестровый номер справочника.)
    @Column(name = "FK_EXT", updatable = false)
    private Integer fkExt;

    // Элементы соответствующие типу
    @OneToMany(mappedBy = "ulistTp", fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Ulist> ulist = new ArrayList<>(0);

    // Организация, владеющая справочником
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_EOLINK", referencedColumnName = "ID")
    private Eolink eolink;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UlistTp ulistTp = (UlistTp) o;
        return id.equals(ulistTp.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

