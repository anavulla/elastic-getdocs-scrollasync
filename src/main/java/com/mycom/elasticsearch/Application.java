package com.mycom.elasticsearch;

import javax.annotation.Resource;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;

/**
 * 
 * @author anavulla
 *
 */
@ComponentScan("com.mycom")
@SpringBootApplication
public class Application implements ApplicationRunner {

	@Resource
	RunApplication runApplication;

	public static void main(String[] args) {
		ApplicationContext ctx = SpringApplication.run(Application.class, args);

		/*
		 * Manually terminating job
		 */
		int exitCode = SpringApplication.exit(ctx, new ExitCodeGenerator() {

			@Override
			public int getExitCode() {
				// TODO Auto-generated method stub
				return 0;
			}
		});
		System.exit(exitCode);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		runApplication.getElasticDocs();

	}

}
