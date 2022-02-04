package com.dic.bill.model.exs;

import com.dic.bill.model.scott.Tuser;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.util.Date;


/**
 * Запросы о задолженности от УСЗН
 */
@SuppressWarnings("serial")
@Entity
@Table(name = "DEBT_SUB_REQUEST", schema = "EXS")
@DynamicUpdate
@Getter
@Setter
public class DebSubRequest implements java.io.Serializable {

    public DebSubRequest() {
    }

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ_DEBT_SUB_REQUEST")
    @SequenceGenerator(name = "SEQ_DEBT_SUB_REQUEST", sequenceName = "EXS.SEQ_DEBT_SUB_REQUEST", allocationSize = 1)
    @Column(name = "id", unique = true, updatable = false, nullable = false)
    private Integer id;

    // идентификатор запроса, присвоенный в ГИС ЖКХ
    @Column(name = "REQUEST_GUID")
    private String requestGuid;

    // номер запроса, присвоенный в ГИС ЖКХ
    @Column(name = "REQUEST_NUMBER")
    private String requestNumber;

    // фамилия
    @Column(name = "LAST_NAME")
    private String lastName;

    // имя
    @Column(name = "FIRST_NAME")
    private String firstName;

    // отчество
    @Column(name = "MIDDLE_NAME")
    private String middleName;

    // снилс
    @Column(name = "SNILS")
    private String snils;

    // документ - серия
    @Column(name = "DOC_SERIA")
    private String docSeria;

    // документ - номер
    @Column(name = "DOC_NUMBER")
    private String docNumber;

    // документ - тип
    @Column(name = "DOC_TYPE")
    private String docType;

    // адрес
    @Column(name = "ADDRESS")
    private String address;

    // адрес - детальный
    @Column(name = "ADDRESS_DETAIL")
    private String addressDetail;

    // дата отправления запроса в ГИС
    @Column(name = "SENT_DATE")
    private Date sentDate;

    // крайний срок ответа на запрос
    @Column(name = "RESPONSE_DATE")
    private Date responseDate;

    // признак наличия задолженности, подтвержденной судебным актом
    @Type(type = "org.hibernate.type.NumericBooleanType")
    @Column(name = "HAS_DEBT")
    private Boolean hasDebt;

    // идентификатор исполнителя, GUID направившего запрос
    @Column(name = "EXECUTOR_GUID")
    private String executorGUID;

    // ФИО Исполнителя направившего запрос
    @Column(name = "EXECUTOR_FIO")
    private String executorFIO;

    // GUID Организации, направившей запрос
    @Column(name = "ORG_FROM_GUID")
    private String orgFromGuid;

    // наименование организации, направившей запрос
    @Column(name = "ORG_FROM_NAME")
    private String orgFromName;

    // телефон организации, направившей запрос
    @Column(name = "ORG_FROM_PHONE")
    private String orgFromPhone;

    // Дополнительная информация по ответу
    @Column(name = "DESCRIPTION")
    private String description;

    // статус загрузки в ГИС (0-принято УК от ГИС, 1-изменено и отправлено в ГИС, 2- принято в ГИС, 3-отозвано УК, 4-отзыв принят ГИС)
    @Column(name = "STATUS")
    private Integer status;

    // статус запроса в ГИС ЖКХ (0- Sent , отправлено в УК, 1-Processed, обработка  (чья???), 2-Revoked, отозвано отправившей организацией)
    @Column(name = "STATUS_GIS")
    private Integer statusGis;

    // статус ответа на запрос (0-NotSent, 1-Sent, 2-AutoGenerated)
    @Column(name = "STATUS_RESPONSE")
    private Integer statusResponse;

    // дом в Eolink
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_EOLINK_HOUSE", referencedColumnName = "ID")
    private Eolink house;

    // пользователь, отправивший ответ на запрос
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_USER_RESPONSE", referencedColumnName = "ID")
    private Tuser user;

    // последний результат отправки в ГИС
    @Column(name = "RESULT")
    private String result;

    // ГИС ЖКХ Транспортный GUID
    @Column(name = "TGUID")
    private String tguid;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DebSubRequest))
            return false;

        DebSubRequest other = (DebSubRequest) o;

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

