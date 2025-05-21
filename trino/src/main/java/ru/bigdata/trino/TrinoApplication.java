package ru.bigdata.trino;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.bigdata.trino.jobs.EtlJobCreateMarts;
import ru.bigdata.trino.jobs.EtlJobGetData;

@SpringBootApplication
public class TrinoApplication implements CommandLineRunner {

	@Autowired
	EtlJobGetData etlJob;

	@Autowired
	EtlJobCreateMarts etlJobCreateMarts;

	public static void main(String[] args) {
		SpringApplication.run(TrinoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// получение job и их вызов
		etlJob.getAllData();
		etlJobCreateMarts.createMarts();

	}
}
