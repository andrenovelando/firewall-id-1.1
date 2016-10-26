/*
 * This code is modified from source provided by Petter Törnberg <pettert@chalmers.se>
 * for the SentiWordNet website.
 * (Original source code: http://sentiwordnet.isti.cnr.it/code/SentiWordNetDemoCode.java)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.firewalld.sentimentanalysis;

import com.firewallid.cleaning.IDRemoval;
import com.firewallid.util.FIConfiguration;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class IDSentiWordNet {

    public static final Logger LOG = LoggerFactory.getLogger(IDSentiWordNet.class);

    public static final String SWN_FILE = "sentimentanalysis.swn.file";

    Configuration firewallConf;
    Map<String, Double> dictionary;
    IDRemoval remove;

    public IDSentiWordNet() throws IOException {
        firewallConf = FIConfiguration.create();
        dictionary = createDictionary();
        remove = new IDRemoval();
    }

    /* Return dictionary */
    private Map<String, Double> createDictionary() throws IOException {
        Map<String, Double> dict = IOUtils
                .readLines(getClass().getClassLoader().getResourceAsStream(firewallConf.get(SWN_FILE)))
                .parallelStream()
                /* If it's a comment, skip this line */
                .filter(line -> !line.trim().startsWith("#"))
                .flatMap(line -> {
                    String[] data = line.split("\t");
                    String wordTypeMarker = data[0];

                    // Example line:
                    // POS ID PosS NegS SynsetTerm#sensenumber Desc
                    // a 00009618 0.5 0.25 spartan#4 austere#3 ascetical#2 ascetic#2 practicing great self-denial;...etc
                    // Is it a valid line? Otherwise, through exception.
                    if (data.length != 6) {
                        throw new IllegalArgumentException(String.format("Incorrect tabulation format in file, line: %s", line));
                    }

                    // Calculate synset score as score = PosS - NegS
                    Double synsetScore = Double.parseDouble(data[2]) - Double.parseDouble(data[3]);

                    // Get all Synset terms
                    String[] synTermsSplit = data[4].split(" ");

                    // Go through all terms of current synset.
                    Stream<Tuple2<String, Tuple2<Double, Double>>> synSets = Arrays.asList(synTermsSplit)
                    .parallelStream()
                    .map(synTermSplit -> {
                        // Get synterm and synterm rank
                        String[] synTermAndRank = synTermSplit.split("#");
                        String synTerm = synTermAndRank[0] + "#" + wordTypeMarker;

                        double synTermRank = Double.parseDouble(synTermAndRank[1]);
                        // What we get here is a (term, (rank, score))
                        return new Tuple2<>(synTerm, new Tuple2<>(synTermRank, synsetScore));
                    });

                    return synSets;
                })
                // What we get here is a map of the type:
                // term -> {score of synset#1, score of synset#2...}
                .collect(Collectors.groupingBy(synSet -> synSet._1,
                                Collectors.mapping(synSet -> synSet._2, Collectors.toList())))
                .entrySet().parallelStream()
                .map(synSet -> {
                    String word = synSet.getKey();
                    List<Tuple2<Double, Double>> synSetScoreList = synSet.getValue();

                    // Calculate weighted average. Weigh the synsets according to
                    // their rank.
                    // Score= 1/2*first + 1/3*second + 1/4*third ..... etc.
                    // Sum = 1/1 + 1/2 + 1/3 ...
                    Tuple2<Double, Double> scoreSum = synSetScoreList.parallelStream()
                    .reduce(new Tuple2<>(0.0, 0.0),
                            (s1, s2) -> new Tuple2<>(
                                    ((s1._1 == 0.0) ? 0.0 : s1._2 / s1._1) + ((s2._1 == 0.0) ? 0.0 : s2._2 / s2._1),
                                    ((s1._1 == 0.0) ? 0.0 : 1 / s1._1) + ((s2._1 == 0.0) ? 0.0 : 1 / s2._1)));

                    double score = scoreSum._1 / scoreSum._2;

                    return new Tuple2<>(word, score);
                })
                .collect(Collectors.toMap(synSet -> synSet._1, synSet -> synSet._2));

        return dict;
    }

    public double extract(String word, String pos) {
        Object scoreObj = dictionary.get(word + "#" + pos);

        if (scoreObj == null) {
            word = remove.stemming(word);
            scoreObj = dictionary.get(word + "#" + pos);
            if (scoreObj == null) {
                scoreObj = 0.0;
            }
        }

        double score = (double) scoreObj;
        if (score >= -0.1 && score <= 0.1) {
            score = 0.0;
        }

        return score;
    }
}
