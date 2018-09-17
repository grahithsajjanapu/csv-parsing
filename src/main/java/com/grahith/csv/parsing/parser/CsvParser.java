package com.grahith.csv.parsing.parser;

import com.grahith.csv.parsing.metrics.Metrics;
import com.grahith.csv.parsing.model.DomainRecord;
import com.grahith.csv.parsing.repository.DomainRepository;
import com.mongodb.DuplicateKeyException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.IncorrectTokenCountException;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Service
@Slf4j
@RequiredArgsConstructor
public class CsvParser {

    private final DomainRepository repository;

    private final Metrics metrics;

    enum Result {
        SUCESS, FATAL_EXCEPTION, WARNING
    }

    @Async ("getAsyncExecutor")
    public CompletableFuture<Path> csvLineParser(Path path, String fileName) throws IOException {
        log.info("Started file: " + fileName + "/" + path.getFileName());
        FlatFileItemReader<DomainRecord> csvFileReader = new FlatFileItemReader<>();
        csvFileReader.setResource(new FileSystemResource(path.toString()));
        csvFileReader.setLineMapper(createLineMapper());

        DomainRecord domainRecord;
        csvFileReader.open(new ExecutionContext());
        Result result = Result.SUCESS;
        boolean eof = true;
        do {
            try {
                domainRecord = csvFileReader.read();
                if (domainRecord != null) {
                    repository.insert(domainRecord);
                } else {
                    eof = false;
                }
            } catch (ParseException | UnexpectedInputException e) {
                log.warn(e.getMessage());
            } catch (DuplicateKeyException e) {
                log.warn(e.getMessage());
                metrics.duplicateKeyExceptionCount();
            } catch (Exception e1) {
                result = Result.FATAL_EXCEPTION;
                log.error(e1.getMessage());
                break;
            }
        } while (eof);
        csvFileReader.close();

        if (result == Result.SUCESS) {
            Path destDir = path.resolveSibling("Completed");
            if (!Files.exists(destDir))
                Files.createDirectory(destDir);
            Files.move(path, destDir.resolve(path.getFileName()), REPLACE_EXISTING);
        } else {
            Path destDir = path.resolveSibling("error");
            if (!Files.exists(destDir))
                Files.createDirectory(destDir);
            Files.move(path, destDir.resolve(path.getFileName()), REPLACE_EXISTING);
        }
        log.info(" Moved file:" + fileName + "/" + path.getFileName());
        return CompletableFuture.completedFuture(path);

    }

    private LineMapper<DomainRecord> createLineMapper() {
        DefaultLineMapper<DomainRecord> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(new DelimitedLineTokenizer());
        lineMapper.setFieldSetMapper(fieldSet -> {
            if (fieldSet.getFieldCount() != 2)
                throw new IncorrectTokenCountException(2, fieldSet.getFieldCount());

            DomainRecord domainRecord = new DomainRecord();
            domainRecord.setDomainName(fieldSet.readString(1));
            domainRecord.setRank(fieldSet.readLong(0));

            return domainRecord;
        });
        return lineMapper;
    }
}
