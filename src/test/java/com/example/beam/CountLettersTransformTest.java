package com.example.beam;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class CountLettersTransformTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCountLettersTransformWithSampleText() {
        List<String> sampleText = List.of(
                "Apple banana apple",
                "Cherry banana cherry cherry"
        );

        PCollection<KV<String, Long>> output = pipeline
                .apply("CreateSampleInput", Create.of(sampleText))
                .apply("CountLetters", new CountLettersTransform());

        PAssert.that(output).containsInAnyOrder(
                KV.of("A", 2L),
                KV.of("B", 2L),
                KV.of("C", 3L)
        );

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testLoremIpsumParagraphsCount() {
        PCollection<KV<String, Long>> output = pipeline
                .apply("CreateLoremInput", Create.of(LoremIpsumWordCountPipeline.LOREM_IPSUM_PARAGRAPHS))
                .apply("CountLetters", new CountLettersTransform());

        PAssert.that(output).containsInAnyOrder(
                KV.of("A", 17L),
                KV.of("B", 1L),
                KV.of("C", 8L),
                KV.of("D", 13L),
                KV.of("E", 20L),
                KV.of("F", 2L),
                KV.of("I", 14L),
                KV.of("L", 6L),
                KV.of("M", 6L),
                KV.of("N", 10L),
                KV.of("O", 4L),
                KV.of("P", 4L),
                KV.of("Q", 12L),
                KV.of("R", 3L),
                KV.of("S", 12L),
                KV.of("T", 3L),
                KV.of("U", 7L),
                KV.of("V", 11L)
        );

        pipeline.run().waitUntilFinish();
    }
}
