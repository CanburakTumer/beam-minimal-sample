package com.example.beam;

import org.apache.beam.sdk.transforms.DoFn;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractInitialLetterFn extends DoFn<String, String> {
    private static final Pattern WORD_PATTERN = Pattern.compile("[a-zA-Z]+");

    @ProcessElement
    public void processElement(@Element String text, OutputReceiver<String> out) {
        if (text == null || text.trim().isEmpty()) {
            return;
        }
        Matcher matcher = WORD_PATTERN.matcher(text);
        while (matcher.find()) {
            String word = matcher.group();
            if (!word.isEmpty()) {
                out.output(word.substring(0, 1).toUpperCase());
            }
        }
    }
}
