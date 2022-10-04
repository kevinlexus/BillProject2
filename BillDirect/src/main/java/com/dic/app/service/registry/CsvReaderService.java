package com.dic.app.service.registry;


import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CsvReaderService {

    @Value("windows-1251")
    private String fileEncoding;

    public List<SberRegistryBean> read(Path file) throws IOException {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
            return read(in);
        }
    }
    private List<SberRegistryBean> read(InputStream is) throws UnsupportedEncodingException {
        Reader reader = new InputStreamReader(is, fileEncoding);
        CSVParser csvParser = new CSVParserBuilder()
                .withSeparator(';')
                .withIgnoreQuotations(true)
                .build();

        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(csvParser)
                .build();
        ColumnPositionMappingStrategy<SberRegistryBean> strat = new ColumnPositionMappingStrategy<>();

        strat.setType(SberRegistryBean.class);
        strat.setColumnMapping(SberRegistryBean.FLDS);

        CsvToBean<SberRegistryBean> csv = new CsvToBean<>();
        csv.setCsvReader(csvReader);
        csv.setMappingStrategy(strat);
        return csv.parse();
    }

}
