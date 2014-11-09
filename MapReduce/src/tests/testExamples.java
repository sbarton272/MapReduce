package tests;

import java.io.IOException;

import master.ConfigLoader;

public class testExamples {

	public static void main(String[] args) {

		ConfigLoader config;
		try {
			config = new ConfigLoader("src/examples/wordcount/wordcount.config");
			System.out.println(config.getJobname());

			// TODO test map and reduce functionality
		} catch (IOException e) {
			System.out.println("Unable to load config");
		}
	}
}
