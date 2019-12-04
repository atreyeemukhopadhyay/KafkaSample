package com.example.kafka.util;

import com.example.kafka.exception.ErrorMessages;
import com.example.kafka.exception.InvalidFileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FileOperations {
    private static Logger log = LoggerFactory.getLogger(FileOperations.class);
    public static Stream<Path> fetchInputFiles(String inputPath) throws InvalidFileException{
        log.info("fetchInputFiles from {}",inputPath);
        try {
            Stream<Path> paths = Files.walk(Paths.get(inputPath)).filter(Files::isRegularFile);
            return paths;
        } catch(IOException ex){
            throw new InvalidFileException(ErrorMessages.FILE_NOT_FOUND.getErrorMessage(),ex.getMessage());
        } catch (Exception ex){
            throw new InvalidFileException(ex.getMessage());
        }
    }

    public static List<String> readInputData(Path path) throws InvalidFileException {
        log.info("readInputData from file {}",path.getFileName());
        List<String> inputLines = new ArrayList<String>();
        try (BufferedReader lines = Files.newBufferedReader(path)) {
            String currentLine;
            while ((currentLine = lines.readLine()) != null) {
                inputLines.add(currentLine);
            }
            log.debug("file data {}",inputLines);
        } catch (IOException ex) {
            throw new InvalidFileException(ErrorMessages.INVALID_FILE_DATA.getErrorMessage(),ex.getMessage());
        }
        return inputLines;

    }
}
