package com.firewallid.cleaning;

import com.firewallid.base.Base;
import com.firewallid.io.HBaseRead;
import com.firewallid.io.HBaseWrite;
import com.firewallid.util.FIFile;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class Cleaning extends Base {

    public static final String TEXT_DELIMITER = "firewallindonesia.file.textdelimiter";
    public static final String NAME_DELIMITER = "firewallindonesia.name.delimiter";

    public static final String CRAWL_SUFFIXNAME = "table.crawl.suffixname";
    public static final String CRAWLING_COLUMN = "table.crawl.crawlingcolumn";
    public static final String CLEANING_COLUMN = "table.crawl.cleaningcolumn";

    public static final String RESOURCES_DIR = "cleaning.resources.dirs";

    static String resources = Cleaning.conf.get(RESOURCES_DIR);
    static IDSentenceDetector isd = createISD();
    static IDFormalization formalization = createFormalization();
    static IDRemoval removal = createRemoval();
    static IDPOSTagger postagger = createPOSTagger();

    boolean remove;

    public Cleaning() {
        super();
        remove = true;
    }

    public void setRemove(boolean remove) {
        this.remove = remove;
    }

    public static void addResources(Class clas, String resources, boolean override) throws IOException, URISyntaxException {
        for (String resource : resources.split(",")) {
            FIFile.copyFolderToWorkingDirectory(clas, resource, override);
        }
    }

    public void addResources(String resources, String destFolder, boolean override) throws IOException, URISyntaxException {
        for (String resource : resources.split(",")) {
            FIFile.copyFolder(resource, destFolder, override);
        }
    }

    public String wordCleaning(String notCleanedWord) {
        String cleanedWordFormal = formalization.normalisasi(formalization.removeSimbol(formalization.standarisasi(formalization.akronimLookUp(formalization.removeURL(formalization.splitAttach(notCleanedWord).toLowerCase()))))); //formalisasi kata
        if (remove) {
            String cleanedWordRemove = removal.stemming(removal.stopwordRemoval(removal.removePunctuation(cleanedWordFormal))); //stopword removal & stemming

            return cleanedWordRemove;
        }
        return cleanedWordFormal;
    }

    public List<String> sentenceCleaning(String notCleanedSentence) {
        //Tokenisasi Kata dengan InaNLP
        List<String> hasilKata = formalization.tokenisasi(notCleanedSentence)
                .parallelStream()
                .map((String kata) -> wordCleaning(kata))
                .filter((String cleanedWord) -> !cleanedWord.isEmpty())
                .collect(Collectors.toList());

        return hasilKata;
    }

    public List<List<String>> textCleaning(String notCleanedText) {
        //Tokenisasi Kalimat dengan InaNLP
        List<List<String>> hasilKalimat = isd.splitSentence(notCleanedText)
                .parallelStream()
                .map(kalimat -> sentenceCleaning(kalimat))
                .filter(cleanedSentence -> !cleanedSentence.isEmpty())
                .collect(Collectors.toList());

        return hasilKalimat;
    }

    public String textCleaningDelimiter(String notCleanedText) {
        List<String> collect = textCleaning(notCleanedText).parallelStream()
                .flatMap(sentence -> sentence.parallelStream())
                .collect(Collectors.toList());

        return Joiner.on(conf.get(TEXT_DELIMITER)).join(collect);
    }

    /* Return post tagger */
    public List<List<Tuple2<String, String>>> textCleaningPOSTagger(String text) {
        List<List<Tuple2<String, String>>> output = textCleaning(text)
                .parallelStream()
                .map(sentence -> {
                    try {
                        return postagger
                                .doPOSTag(Joiner.on(" ").join(sentence))
                                .parallelStream()
                                .map(wordTag -> new Tuple2<>(wordTag[0], wordTag[1]))
                                .collect(Collectors.toList());
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                        LOG.error(ex.getMessage());
                    }
                    return null;
                })
                .collect(Collectors.toList());

        return output;
    }

    public Tuple2<String, String> tupleCleaning(Tuple2<String, String> notCleanedTuple) {
        if (notCleanedTuple._2 == null || notCleanedTuple._2.isEmpty()) {
            return null;
        }
        return new Tuple2(notCleanedTuple._1, textCleaningDelimiter(notCleanedTuple._2));
    }

    static IDSentenceDetector createISD() {
        try {
            addResources(Cleaning.class, resources, true);
        } catch (IOException | URISyntaxException ex) {
            System.out.println(ex.getMessage());
        }
        return new IDSentenceDetector();
    }

    static IDFormalization createFormalization() {
        try {
            addResources(Cleaning.class, resources, true);
        } catch (IOException | URISyntaxException ex) {
            System.out.println(ex.getMessage());
        }
        return new IDFormalization();
    }

    static IDRemoval createRemoval() {
        try {
            addResources(Cleaning.class, resources, true);
        } catch (IOException | URISyntaxException ex) {
            System.out.println(ex.getMessage());
        }
        return new IDRemoval();
    }

    static IDPOSTagger createPOSTagger() {
        try {
            addResources(Cleaning.class, resources, true);
        } catch (IOException | URISyntaxException ex) {
//            LOG.error(ex.getMessage());
        }
        try {
            return new IDPOSTagger();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException ex) {
//            LOG.error(ex.getMessage());
            return null;
        }
    }

    public JavaPairRDD<String, String> doCleaning(JavaPairRDD<String, String> notCleanedRow) throws IOException, URISyntaxException {
        JavaPairRDD<String, String> cleanedRow = notCleanedRow
                .mapToPair(row -> tupleCleaning(row))
                .filter(row -> row != null);
        return cleanedRow;
    }

    public void run(JavaSparkContext jsc, String type, String prefixTableName) throws IOException, URISyntaxException {
        conf.addResource(type + "-default.xml");
        String tableName = prefixTableName + conf.get(NAME_DELIMITER) + conf.get(CRAWL_SUFFIXNAME);
        String crawlingColumn = conf.get(CRAWLING_COLUMN);
        String cleaningColumn = conf.get(CLEANING_COLUMN);

        /* Get data from HBase */
        JavaPairRDD<String, String> notCleanedRow = new HBaseRead().doRead(jsc, tableName, crawlingColumn, cleaningColumn);

        /* Run cleaning */
        JavaPairRDD<String, String> cleanedRow = doCleaning(notCleanedRow);

        /* Save to Hbase */
        new HBaseWrite().save(tableName, cleanedRow, cleaningColumn);
    }
}
