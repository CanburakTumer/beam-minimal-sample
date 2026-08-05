package com.example.beam;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface LoremIpsumWordCountOptions extends PipelineOptions {

    @Description("Target GCS bucket or base path (e.g. gs://my-bucket)")
    @Required
    String getOutputBucket();

    void setOutputBucket(String value);
}
