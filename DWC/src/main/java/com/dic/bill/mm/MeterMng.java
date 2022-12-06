package com.dic.bill.mm;

import com.dic.bill.dto.MeterData;
import com.dic.bill.dto.UslMeterVol;
import com.ric.dto.ListMeter;
import com.ric.dto.MapMeter;
import com.ric.dto.SumMeterVol;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Meter;
import com.dic.bill.model.scott.ObjPar;
import org.springframework.transaction.annotation.Transactional;

import javax.xml.datatype.XMLGregorianCalendar;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public interface MeterMng {

    boolean getIsMeterDataExist(List<MeterData> lst, String guid, XMLGregorianCalendar ts);

    boolean getIsMeterOpenForReceiveData(Meter meter);

    boolean getIsMeterOpenForSendData(Meter meter);

    boolean getCanSaveDataMeter(Eolink meterEol, Date dt);

    List<ObjPar> getValuesByMeter(Meter meter, int status, String period);

    List<Meter> findMeter(int i, int i1) throws InterruptedException;

    Optional<Meter> getActualMeterByKoUsl(Ko ko, String usl, Date dt);

    Optional<Meter> getActualMeterByKo(Ko koPremis, String usl, Date dt);

    UslMeterVol getPartDayMeterVol(List<SumMeterVol> lstMeterVol, Date dtFrom, Date dtTo);

    boolean isExistAnyMeter(List<SumMeterVol> lstMeterVol, String uslId, Date dt);

    int sendMeterVal(BufferedWriter writer, String lsk, String strUsl,
                     String prevValue, String value, String period,
                     int userId, int docParId,
                     boolean isSetPreviosVal) throws IOException;

    ListMeter getListMeterByKlskId(Long koObjId, Date dt1, Date dt2);

    MapMeter getMapMeterByKlskId(Long koObjId, Date dt1, Date dt2);

    Integer saveMeterValByKLskId(Long klskId, Double curVal);

    @Transactional
    Integer saveMeterValByMeterId(int meterId, double curVal);
}
