package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.GenPenMng;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.SprPenKey;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.SprPen;
import com.dic.bill.model.scott.Stavr;
import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.Optional;

/**
 * Сервис расчета пени
 *
 * @author Lev
 * @version 2.00
 */
@Slf4j
@Service
public class GenPenMngImpl implements GenPenMng {

    private final ConfigApp configApp;

    public GenPenMngImpl(ConfigApp configApp) {
        this.configApp = configApp;
    }


    /**
     * Внутренний класс DTO расчета пени
     *
     * @author Lev
     */
    @Getter
    @Setter
    public class PenDTO {
        // кол-во дней просрочки
        int days = 0;
        // рассчитанная пеня
        BigDecimal penya;
        // % по которому рассчитана пеня (информационно)
        BigDecimal proc;
        // ставка рефинансирования, по которой расчитана пеня
        Stavr stavr;
    }

    /**
     * Рассчитать пеню
     *
     * @param deb  - долг
     * @param mg   - период долга
     * @param kart - лиц.счет
     * @return
     */
    @Override
    public Optional<PenDTO> calcPen(CalcStore calcStore, BigDecimal deb, Integer mg, Kart kart, Date curDt) {
        // дата начала начисления пени
        SprPen penDt = getPenDt(mg, kart);
        // вернуть кол-во дней между датой расчета пени и датой начала пени по справочнику
        if (penDt == null) {
            // некритическая ошибка отсутствия записи в справочнике пени, просто не начислить пеню!
            //log.warn("ОШИБКА во время начисления пени по лиц.счету lsk={}, возможно не настроен справочник C_SPR_PEN!"
            //                + "Попытка найти элемент: mg={}, kart.tp={}, kart.reu={}", kart.getLsk(),
            //        mg, kart.getTp().getId(), kart.getUk().getReu());
            return Optional.empty();
        }
        int days = Utl.daysBetween(penDt.getDt(), curDt);
        PenDTO penDTO = new PenDTO();
        penDTO.proc = BigDecimal.ZERO;
        penDTO.penya = BigDecimal.ZERO;
        penDTO.days = days;
        if (days > 0 && !(kart.getPnDt() != null && curDt.getTime() >= kart.getPnDt().getTime())) {
            // пеня возможна, если есть кол-во дней долга
            //log.info(" spr={}, cur={}, curDays={}", sprPenUsl.getTs(), curDt, curDays);
            Stavr stavr = configApp.getLstStavr().stream()
                    .filter(t -> t.getTp().equals(kart.getTp())) // фильтр по типу лиц.счета
                    .filter(t -> days >= t.getDays1() && days <= t.getDays2()) // фильтр по кол-ву дней долга
                    .filter(t -> Utl.between(curDt, t.getDt1(), t.getDt2())) // фильтр по дате расчета в справочнике
                    .findFirst().orElse(null);
            penDTO.days = days;
            if (stavr != null) {
                // расчет пени = долг * процент/100
                penDTO.proc = stavr.getProc();
                penDTO.penya = deb.multiply(penDTO.proc).divide(new BigDecimal(100), 10, RoundingMode.HALF_UP);
                penDTO.stavr = stavr;
            }
        }
        return Optional.of(penDTO);

    }

    /**
     * Получить строку даты начала пени по типу лиц.счета
     *
     * @param mg        - период задолженности
     * @param kart      - лиц.счет
     */
    private SprPen getPenDt(Integer mg, Kart kart) {
        return configApp.getMapSprPen().get(new SprPenKey(kart.getTp(), mg, kart.getUk().getReu()));
    }

}
