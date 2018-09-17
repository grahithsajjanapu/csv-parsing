package com.grahith.csv.parsing.parser;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class CsvWriter {

    private final CsvParser csvParser;

    private HashSet<Path> setOfPaths = new HashSet<>();

    @Value("${spring.parser.rootDirectory}")
    String rootPath;

    private SimpleFileVisitor<Path> fileVisitor = new SimpleFileVisitor<Path>() {

        @Override
        public FileVisitResult visitFile(Path dir, BasicFileAttributes attrs) throws IOException {
            if(Files.isRegularFile(dir)
                    && FilenameUtils.isExtension(dir.getFileName().toString(), "csv")
                    && !setOfPaths.contains(dir)) {
                try {
                    CompletableFuture<Path> cf = csvParser.csvLineParser(dir, dir.getParent().getFileName().toString());
                    setOfPaths.add(dir);
                    cf.whenComplete((path, throwable) -> {
                        setOfPaths.remove(dir);
                        if(throwable != null)
                            log.error(throwable.getMessage());
                    });
                } catch (TaskRejectedException e) {
                    log.debug(e.getMessage());
                }

            }
            return FileVisitResult.CONTINUE;
        }
    };

    @Scheduled(fixedDelay = 600000L, initialDelay = 5000L)
    public void walkFileTree() throws IOException {
        if(!Files.isDirectory(Paths.get(rootPath))) {
            log.error("Root Path is NOT valid, skipping csv scanning");
        } else {
            log.info("Started Scheduled CSV scanner");
            Files.walkFileTree(Paths.get(rootPath), EnumSet.noneOf(FileVisitOption.class), 2, fileVisitor);
        }
    }
}
