package com.dic.app.mm.impl.bot;

import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
class Column {
    String name;
    String caption;

    String captionPrefixed;
    Integer size;

    Column(String name, String caption) {
        this.name = name;
        this.caption = caption;
        this.size = caption.length();
    }

    String getCaptionWithPrefix() {
        return Utl.getStrPrefixed(caption, size, " ");
    }

    String getValueFormatted(Double val, String pattern) {
        return Utl.getMoneyStr(val, size, " ", pattern);
    }

    String getStrFormatted(String str) {
        return Utl.getStrPrefixed(str, size, " ");
    }
}
