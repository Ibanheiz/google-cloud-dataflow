package com.ibanheiz.gcp.mapreducelab;

import java.util.Arrays;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */
public class StarterPipeline {
	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.create();
		DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
		dataflowOptions.setRunner(DataflowRunner.class);
		dataflowOptions.setProject("curso-gcloud-187323");
		dataflowOptions.setTempLocation("gs://ibanheiz-dataflow-bucket/tmp");
		dataflowOptions.setJobName("Teste-from-eclipse");

		Pipeline p = Pipeline.create(options);

		p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
				.apply(FlatMapElements.into(TypeDescriptors.strings())
						.via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
				.apply(Filter.by((String word) -> !word.isEmpty()))
				.apply(Count.<String>perElement())
				.apply(MapElements.into(TypeDescriptors.strings())
						.via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
				.apply(TextIO.write().to("gs://ibanheiz-dataflow-bucket/lab/test-"));

		p.run().waitUntilFinish();
	}
}
