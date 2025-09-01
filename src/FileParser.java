import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public class FileParser {

    private static final String DEFAULT_OUTPUT_DIR = ".";
    private static final String DEFAULT_PREFIX = "";

    private static final String INTEGERS_FILENAME = "integers.txt";
    private static final String FLOATS_FILENAME = "floats.txt";
    private static final String STRINGS_FILENAME = "strings.txt";

    private static final Pattern INTEGER_PATTERN = Pattern.compile("[+-]?\\d+");
    private static final Pattern FLOAT_PATTERN = Pattern.compile("[+-]?(\\d+\\.\\d*|\\.\\d+)([eE][+-]?\\d+)?");

    private enum DataType {
        INTEGER, FLOAT, STRING
    }

    // Класс для сбора статистики обрабатываемых данных
    private static class Statistics {
        long count = 0;

        BigInteger minInt = null;
        BigInteger maxInt = null;
        BigInteger sumInt = BigInteger.ZERO;


        BigDecimal minFloat = null;
        BigDecimal maxFloat = null;
        BigDecimal sumFloat = BigDecimal.ZERO;

        OptionalInt minStringLength = OptionalInt.empty();
        OptionalInt maxStringLength = OptionalInt.empty();

        List<String> lines = new ArrayList<>();

        synchronized void addInt(String value) {
            lines.add(value);
            count++;
            BigInteger unitInt = new BigInteger(value);
            if (minInt == null) {
                minInt = unitInt;
            } else if (unitInt.compareTo(minInt) < 0) {
                minInt = unitInt;
            }
            if (maxInt == null) {
                maxInt = unitInt;
            } else if (unitInt.compareTo(maxInt) > 0) {
                maxInt = unitInt;
            }
            sumInt = sumInt.add(unitInt);
        }

        synchronized void addFloat(String value) {
            lines.add(value);
            count++;
            BigDecimal unitFloat = new BigDecimal(value);
            if (minFloat == null) {
                minFloat = unitFloat;
            } else if (unitFloat.compareTo(minFloat) < 0) {
                minFloat = unitFloat;
            }
            if (maxFloat == null) {
                maxFloat = unitFloat;
            } else if (unitFloat.compareTo(maxFloat) > 0) {
                maxFloat = unitFloat;
            }
            sumFloat = sumFloat.add(unitFloat);
        }

        synchronized void addString(String value) {
            lines.add(value);
            int lengthString = value.length();
            count++;
            if (minStringLength.isEmpty()) {
                minStringLength = OptionalInt.of(lengthString);
            } else if (lengthString < minStringLength.getAsInt()) {
                minStringLength = OptionalInt.of(lengthString);
            }
            if (maxStringLength.isEmpty()) {
                maxStringLength = OptionalInt.of(lengthString);
            } else if (lengthString > maxStringLength.getAsInt()) {
                maxStringLength = OptionalInt.of(lengthString);
            }
        }

        void printShort(String shortInfo) {
            System.out.println(shortInfo + ": " + count);
        }

        void printFull(String fullInfo) {
            System.out.println(fullInfo + ":");
            System.out.println("  count: " + count);
            switch (fullInfo) {
                case "integers" -> {
                    if (minInt != null) {
                        System.out.println("  min: " + minInt);
                    }
                    if (maxInt != null) {
                        System.out.println("  max: " + maxInt);
                    }
                    System.out.println("  sum: " + sumInt);
                    String average;
                    if (count > 0) {
                        BigDecimal avg = new BigDecimal(sumInt).divide(BigDecimal.valueOf(count), 10, java.math.RoundingMode.HALF_UP);
                        average = avg.toString();
                    } else {
                        average = "0";
                    }
                    System.out.println("  average: " + average);
                }
                case "floats" -> {
                    if (minFloat != null) {
                        System.out.println("  min: " + minFloat);
                    }
                    if (maxFloat != null) {
                        System.out.println("  max: " + maxFloat);
                    }
                    System.out.println("  sum: " + sumFloat);
                    String average;
                    if (count > 0) {
                        BigDecimal avg = sumFloat.divide(BigDecimal.valueOf(count), 10, java.math.RoundingMode.HALF_UP);
                        average = avg.toString();
                    } else {
                        average = "0";
                    }
                    System.out.println("  average: " + average);
                }
                case "strings" -> {
                    if (minStringLength.isPresent()) {
                        System.out.println("  shortest: " + minStringLength.getAsInt());
                    }
                    if (maxStringLength.isPresent()) {
                        System.out.println("  longest: " + maxStringLength.getAsInt());
                    }
                }
            }
        }

        List<String> getLines() {
            return lines;
        }

        // Методы для объединения статистики из разных потоков
        synchronized void merge(Statistics other) {
            this.lines.addAll(other.lines);
            this.count += other.count;

            // Объединяем статистику для целых чисел
            if (other.minInt != null) {
                if (this.minInt == null || other.minInt.compareTo(this.minInt) < 0) {
                    this.minInt = other.minInt;
                }
            }
            if (other.maxInt != null) {
                if (this.maxInt == null || other.maxInt.compareTo(this.maxInt) > 0) {
                    this.maxInt = other.maxInt;
                }
            }
            this.sumInt = this.sumInt.add(other.sumInt);

            // Объединяем статистику для вещественных чисел
            if (other.minFloat != null) {
                if (this.minFloat == null || other.minFloat.compareTo(this.minFloat) < 0) {
                    this.minFloat = other.minFloat;
                }
            }
            if (other.maxFloat != null) {
                if (this.maxFloat == null || other.maxFloat.compareTo(this.maxFloat) > 0) {
                    this.maxFloat = other.maxFloat;
                }
            }
            this.sumFloat = this.sumFloat.add(other.sumFloat);

            // Объединяем статистику для строк
            if (other.minStringLength.isPresent()) {
                if (this.minStringLength.isEmpty() || other.minStringLength.getAsInt() < this.minStringLength.getAsInt()) {
                    this.minStringLength = other.minStringLength;
                }
            }
            if (other.maxStringLength.isPresent()) {
                if (this.maxStringLength.isEmpty() || other.maxStringLength.getAsInt() > this.maxStringLength.getAsInt()) {
                    this.maxStringLength = other.maxStringLength;
                }
            }
        }
    }

    public static void main(String[] args) {
        FileParser filter = new FileParser();
        filter.run(args);
    }

    // Функция обработки файлов
    public void run(String[] args) {
        try {
            FileInfo parsedArgs = parseArguments(args);
            if (parsedArgs.inputFiles.isEmpty()) {
                System.err.println("Ошибка: не указаны входные файлы.");
                printUsage();
                return;
            }

            String outputDir = parsedArgs.outputDir;
            String prefix = parsedArgs.prefix;
            boolean appendMode = parsedArgs.appendMode;
            boolean shortStat = parsedArgs.shortStat;
            boolean fullStat = parsedArgs.fullStat;

            // Создаем директорию вывода, если ее не существует
            Path outputDirPath = Paths.get(outputDir);
            if (!Files.exists(outputDirPath)) {
                try {
                    Files.createDirectories(outputDirPath);
                } catch (IOException e) {
                    System.err.printf("Ошибка: не удалось создать директорию вывода '"
                            + outputDirPath
                            + "': "
                            + e.getMessage());
                    return;
                }
            }

            // Многопоточная обработка файлов
            ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            List<Future<Map<DataType, Statistics>>> futures = new ArrayList<>();

            // Запускаем обработку каждого файла в отдельном потоке
            for (String fileName : parsedArgs.inputFiles) {
                Future<Map<DataType, Statistics>> future = executor.submit(() -> processFile(fileName));
                futures.add(future);
            }

            // Собираем результаты из всех потоков
            var globalStatistics = new EnumMap<DataType, Statistics>(DataType.class);
            globalStatistics.put(DataType.INTEGER, new Statistics());
            globalStatistics.put(DataType.FLOAT, new Statistics());
            globalStatistics.put(DataType.STRING, new Statistics());

            for (Future<Map<DataType, Statistics>> future : futures) {
                try {
                    Map<DataType, Statistics> fileStats = future.get();
                    for (DataType type : DataType.values()) {
                        globalStatistics.get(type).merge(fileStats.get(type));
                    }
                } catch (ExecutionException e) {
                    System.err.println("Ошибка при обработке файла: " + e.getCause().getMessage() + "\n");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Обработка прервана" + "\n");
                }
            }

            executor.shutdown();

            writeResults(outputDirPath, prefix, appendMode, globalStatistics);

            printStatistics(globalStatistics, shortStat, fullStat);

        } catch (IllegalArgumentException e) {
            System.err.println("Ошибка аргументов: " + e.getMessage() + "\n");
            printUsage();
        } catch (Exception e) {
            System.err.println("Неожиданная ошибка: " + e.getMessage() + "\n");
            e.printStackTrace();
        }
    }

    private FileInfo parseArguments(String[] args) {
        FileInfo result = new FileInfo();
        int i = 0;

        while (i < args.length) {
            String arg = args[i];

            switch (arg) {
                case "-o":
                    if (i + 1 >= args.length) throw new IllegalArgumentException("-o требует путь");
                    result.outputDir = args[++i];
                    i++;
                    break;

                case "-p":
                    if (i + 1 >= args.length) throw new IllegalArgumentException("-p требует префикс");
                    result.prefix = args[++i];
                    i++;
                    break;

                case "-a":
                    result.appendMode = true;
                    i++;
                    break;

                case "-s":
                    result.shortStat = true;
                    i++;
                    break;

                case "-f":
                    result.fullStat = true;
                    i++;
                    break;

                default:
                    if (arg.startsWith("-")) {
                        throw new IllegalArgumentException("Неизвестная опция: " + arg);
                    } else {
                        result.inputFiles.add(arg);
                        i++;
                    }
            }
        }

        if (result.shortStat && result.fullStat) {
            throw new IllegalArgumentException("Опции -s и -f не могут использоваться одновременно");
        }

        return result;
    }

    private static Map<DataType, Statistics> processFile(String fileName) {
        var fileStats = new EnumMap<DataType, Statistics>(DataType.class);
        fileStats.put(DataType.INTEGER, new Statistics());
        fileStats.put(DataType.FLOAT, new Statistics());
        fileStats.put(DataType.STRING, new Statistics());

        Path path = Paths.get(fileName);
        if (!Files.exists(path)) {
            System.err.printf("Предупреждение: файл '" + fileName + "' не существует, пропускаем его." + "\n");
            return fileStats;
        }

        if (Files.isDirectory(path)) {
            System.err.printf("Предупреждение: '" + fileName + "' — это директория, а не файл." + "\n");
            return fileStats;
        }

        String line = null;
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                DataType type = classify(line);

                switch (type) {
                    case INTEGER -> fileStats.get(type).addInt(line);
                    case FLOAT -> fileStats.get(type).addFloat(line);
                    case STRING -> fileStats.get(type).addString(line);
                }
            }
        } catch (IOException e) {
            System.err.printf("Ошибка при чтении файла '" + fileName + "': " + e.getMessage() + "\n");
        } catch (Exception e) {
            System.err.printf("Ошибка обработки строки '" + line + "' в файле '" + fileName + "': " + e.getMessage() + "\n");
        }

        return fileStats;
    }

    private static DataType classify(String line) {
        if (INTEGER_PATTERN.matcher(line).matches()) {
            try {
                new BigInteger(line);
                return DataType.INTEGER;
            } catch (NumberFormatException ignored) {
                // Если не удалось распарсить как BigInteger, проверяем дальше
            }
        }

        if (FLOAT_PATTERN.matcher(line).matches()) {
            try {
                new BigDecimal(line);
                return DataType.FLOAT;
            } catch (NumberFormatException ignored) {
                // Если не удалось распарсить как BigDecimal, считаем строкой
            }
        }

        return DataType.STRING;
    }

    private void writeResults(Path outputDir, String prefix, boolean appendMode, Map<DataType, Statistics> stats) {
        try {
            boolean integersWritten = writeToFile(outputDir, prefix + INTEGERS_FILENAME,
                    stats.get(DataType.INTEGER).getLines(), appendMode);
            boolean floatsWritten = writeToFile(outputDir, prefix + FLOATS_FILENAME,
                    stats.get(DataType.FLOAT).getLines(), appendMode);
            boolean stringsWritten = writeToFile(outputDir, prefix + STRINGS_FILENAME,
                    stats.get(DataType.STRING).getLines(), appendMode);

            // Удаляем файлы, если они были созданы пустыми в режиме append
            if (!integersWritten) {
                if (stats.get(DataType.INTEGER).count == 0) {
                    Files.deleteIfExists(outputDir.resolve(prefix + INTEGERS_FILENAME));
                }
            }
            if (!floatsWritten) {
                if (stats.get(DataType.FLOAT).count == 0) {
                    Files.deleteIfExists(outputDir.resolve(prefix + FLOATS_FILENAME));
                }
            }
            if (!stringsWritten) {
                if (stats.get(DataType.STRING).count == 0) {
                    Files.deleteIfExists(outputDir.resolve(prefix + STRINGS_FILENAME));
                }
            }
        } catch (IOException e) {
            System.err.printf("Ошибка при записи результатов: " + e.getMessage());
        }
    }

    private boolean writeToFile(Path dir, String fileName, List<String> lines, boolean append) throws IOException {
        if (lines.isEmpty()) return false;

        Path path = dir.resolve(fileName);
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE,
                append ? StandardOpenOption.APPEND
                        : StandardOpenOption.TRUNCATE_EXISTING)) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
        return true;
    }

    private void printStatistics(Map<DataType, Statistics> stats, boolean shortStat, boolean fullStat) {
        System.out.println("=== Статистика ===");

        boolean showStats = shortStat || fullStat;

        for (var entry : stats.entrySet()) {
            DataType type = entry.getKey();
            Statistics s = entry.getValue();

            String typeName = "";
            if (type == DataType.INTEGER) {
                typeName = "integers";
            } else if (type == DataType.FLOAT) {
                typeName = "floats";
            } else if (type == DataType.STRING) {
                typeName = "strings";
            }

            if (showStats) {
                if (s.count > 0) {
                    if (fullStat) {
                        s.printFull(typeName);
                    } else {
                        s.printShort(typeName);
                    }
                }
            } else {
                // По умолчанию — краткая статистика
                if (s.count > 0) {
                    s.printShort(typeName);
                }
            }
        }
    }

    private void printUsage() {
        System.out.println(
                """
                        Использование: java DataFilter [опции] <файл1> [файл2 ...]
                        Опции:
                          -o <путь>     — путь для выходных файлов (по умолчанию: .)
                          -p <префикс>  — префикс имен выходных файлов (по умолчанию: пусто)
                          -a            — режим добавления к существующим файлам
                          -s            — краткая статистика (только количество)
                          -f            — полная статистика (мин, макс, сумма, среднее, длины)
                        
                        Выходные файлы: <префикс>integers.txt, <префикс>floats.txt, <префикс>strings.txt
                        """
        );
    }

    // Класс хранения аргументов для работы с файлами
    private static class FileInfo {
        List<String> inputFiles = new ArrayList<>();
        String outputDir = DEFAULT_OUTPUT_DIR;
        String prefix = DEFAULT_PREFIX;
        boolean appendMode = false;
        boolean shortStat = false;
        boolean fullStat = false;
    }
}