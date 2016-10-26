/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewalld.sentimentanalysis;

import com.firewallid.cleaning.Cleaning;
import com.firewallid.util.FIConfiguration;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

/**
 *
 * @author andrenovelando
 */
public class SentimentAnalysis {

    public static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysis.class);

    Configuration firewallConf;
    Cleaning cleaning;
    IDSentiWordNet idswn;

    public SentimentAnalysis() {
        firewallConf = FIConfiguration.create();
        cleaning = new Cleaning();
        cleaning.setRemove(false);
        try {
            idswn = new IDSentiWordNet();
        } catch (IOException ex) {
            LOG.error(ex.getMessage());
        }
    }

    /* Convertion InaNLP's POS into IDSentiWordNet's POS*/
    public String standardPOS(String pos) {
        if (pos.matches("NN|PRP|NNP|NNG")) {
            pos = "n";
        } else if (pos.matches("JJ")) {
            pos = "a";
        } else if (pos.matches("RB")) {
            pos = "r";
        } else if (pos.matches("VBI|VBT")) {
            pos = "v";
        }

        return pos;
    }

    public List<Tuple3<String, String, Double>> scoreIDSWN(List<Tuple2<String, String>> sentence) {
        List<Tuple3<String, String, Double>> sentenceScore = sentence
                .parallelStream()
                .map(word -> new Tuple3<>(
                        word._1,
                        word._2,
                        idswn.extract(word._1, standardPOS(word._2))
                ))
                .collect(Collectors.toList());

        LOG.info(String.format("IDSentiWordNet Score: %s", Joiner.on(" ").join(sentenceScore)));

        return sentenceScore;
    }

    public List<Tuple3<String, String, Double>> scoreNegationEvaluation(List<Tuple3<String, String, Double>> sentence) {
        int count = 0;
        boolean negation = false;

        for (int i = 0; i < sentence.size(); i++) {
            Tuple3<String, String, Double> word = sentence.get(i);
            double score = word._3();

            switch (word._2()) {
                case "NEG":
                    negation ^= true;
                case "CC":
                    negation = false;
                default:
                    if (negation && count < 4) {
                        score *= -1;
                        count++;
                    }
                    if (count == 4) {
                        count = 0;
                        negation = false;
                    }
            }

            sentence.set(i, new Tuple3<>(word._1(), word._2(), score));
        }

        LOG.info(String.format("Negation Evaluation Score: %s", Joiner.on(" ").join(sentence)));

        return sentence;
    }

    public Tuple2<Double, Double> scoreAverage(List<Tuple3<String, String, Double>> sentence) {
        Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> posNegTotal = sentence
                .parallelStream()
                .filter(word -> word._3() != 0)
                /* Map of ((pos_score, pos_size), (neg_score, neg_size))*/
                .map(word -> {
                    Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> posNeg;
                    Tuple2<Double, Integer> score = new Tuple2<>(word._3(), 1);
                    Tuple2<Double, Integer> zero = new Tuple2<>(0.0, 0);

                    if (word._3() > 0) {
                        posNeg = new Tuple2<>(score, zero);
                    } else {
                        posNeg = new Tuple2<>(zero, score);
                    }
                    return posNeg;
                })
                /* Reduce to ((total_pos_score, total_pos_size), (total_neg_score, total_neg_size)) */
                .reduce(new Tuple2<>(new Tuple2<>(0.0, 0), new Tuple2<>(0.0, 0)),
                        (posNeg1, posNeg2) -> new Tuple2<>(
                                new Tuple2<>(posNeg1._1._1 + posNeg2._1._1,
                                        posNeg1._1._2 + posNeg2._1._2),
                                new Tuple2<>(posNeg1._2._1 + posNeg2._2._1,
                                        posNeg1._2._2 + posNeg2._2._2)
                        ));

        Tuple2<Double, Double> average = new Tuple2<>(
                (posNegTotal._1._2 == 0) ? 0.0 : posNegTotal._1._1 / posNegTotal._1._2,
                (posNegTotal._2._2 == 0) ? 0.0 : posNegTotal._2._1 / posNegTotal._2._2 * -1);

        LOG.info(String.format("Average Score (positif, negatif): %s -> %s",
                Joiner.on(" ").join(sentence),
                average));

        return average;
    }

    public Tuple2<Double, Double> getScore(List<Tuple2<String, String>> sentence) {
        List<Tuple3<String, String, Double>> scoreIDSWN = scoreIDSWN(sentence);
        List<Tuple3<String, String, Double>> scoreNegationEvaluation = scoreNegationEvaluation(scoreIDSWN);
        Tuple2<Double, Double> scoreAverage = scoreAverage(scoreNegationEvaluation);

        return scoreAverage;
    }

    public String detect(String text) {
        Tuple2<Double, Double> scoreTotal = cleaning.textCleaningPOSTagger(text)
                .parallelStream()
                .map(sentence -> getScore(sentence))
                .reduce(new Tuple2<>(0.0, 0.0),
                        (score1, score2)
                        -> new Tuple2<>(score1._1 + score2._1, score1._2 + score2._2));

        String sentiment;

        if (scoreTotal._1 > scoreTotal._2) {
            sentiment = "positif";
        } else if (scoreTotal._1 < scoreTotal._2) {
            sentiment = "negatif";
        } else {
            sentiment = "netral";
        }

        LOG.info(String.format("Text: %s. Total score: %s. Sentiment: %s",
                (text.length() < 100) ? text : text.substring(0, 100) + "... ",
                scoreTotal,
                sentiment));

        return sentiment;
    }
}
