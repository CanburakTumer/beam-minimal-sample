package com.example.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class FormatYamlFn extends DoFn<KV<String, Long>, String> {

    @ProcessElement
    public void processElement(@Element KV<String, Long> kv, OutputReceiver<String> out) {
        if (kv != null && kv.getKey() != null && kv.getValue() != null) {
            out.output(kv.getKey() + ": " + kv.getValue());
        }
    }
}
