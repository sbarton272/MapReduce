package tests;

import java.io.IOException;

import master.ConfigLoader;

public class testExamples {

	public static void main(String[] args) {

		// Run Word Count
		ConfigLoader config;
		try {
			config = new ConfigLoader("examples/wordcount/wordcount.config");
			System.out.println(config.getJobname());

			// TODO test map and reduce functionality
		} catch (IOException e) {
			System.out.println("Unable to load config");
		}

		// Run Word Occurrences
		try {
			config = new ConfigLoader("examples/wordoccurences/wordoccurences.config");
			System.out.println(config.getJobname());

			// TODO test map and reduce functionality
		} catch (IOException e) {
			System.out.println("Unable to load config");
		}


	}
}
