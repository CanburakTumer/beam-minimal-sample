package com.example.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LoremIpsumWordCountPipeline {

    public static final List<String> LOREM_IPSUM_PARAGRAPHS = List.of(
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
            "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem."
    );

    public static class CollectAndSortCombineFn extends CombineFn<KV<String, Long>, List<KV<String, Long>>, List<KV<String, Long>>> {
        @Override
        public List<KV<String, Long>> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<KV<String, Long>> addInput(List<KV<String, Long>> accum, KV<String, Long> input) {
            accum.add(input);
            return accum;
        }

        @Override
        public List<KV<String, Long>> mergeAccumulators(Iterable<List<KV<String, Long>>> accums) {
            List<KV<String, Long>> result = new ArrayList<>();
            for (List<KV<String, Long>> a : accums) {
                result.addAll(a);
            }
            return result;
        }

        @Override
        public List<KV<String, Long>> extractOutput(List<KV<String, Long>> accum) {
            accum.sort(Comparator.comparing(KV::getKey));
            return accum;
        }
    }

    public static void main(String[] args) {
        LoremIpsumWordCountOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(LoremIpsumWordCountOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        String basePath = options.getOutputBucket();
        if (basePath.endsWith("/")) {
            basePath = basePath.substring(0, basePath.length() - 1);
        }

        pipeline
                .apply("CreateInput", Create.of(LOREM_IPSUM_PARAGRAPHS))
                .apply("CountLetters", new CountLettersTransform())
                .apply("CollectAndSort", Combine.globally(new CollectAndSortCombineFn()))
                .apply("FormatYaml", ParDo.of(new DoFn<List<KV<String, Long>>, String>() {
                    @ProcessElement
                    public void processElement(@Element List<KV<String, Long>> sortedList, OutputReceiver<String> out) {
                        if (sortedList != null) {
                            for (KV<String, Long> kv : sortedList) {
                                out.output(kv.getKey() + ": " + kv.getValue());
                            }
                        }
                    }
                }))
                .apply("WriteOutput", FileIO.<String>write()
                        .via(TextIO.sink())
                        .to(basePath)
                        .withNaming((window, pane, numShards, shardIndex, compression) -> {
                            Instant now = Instant.now();
                            ZonedDateTime zdt = now.atZone(ZoneId.of("UTC"));
                            String datePath = zdt.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
                            String timeFileName = zdt.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                            return datePath + "/" + timeFileName + ".yaml";
                        })
                        .withNumShards(1)
                );

        pipeline.run().waitUntilFinish();
    }
}
