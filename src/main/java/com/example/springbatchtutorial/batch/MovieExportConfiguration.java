package com.example.springbatchtutorial.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class MovieExportConfiguration {

    @Bean
    public Job movieExportJob(JobRepository jobRepository, Step movieExportStep) {
        return new JobBuilder("movieExportJob", jobRepository)
                .start(movieExportStep)
                .build();
    }

    @Bean
    public Step movieExportStep(JobRepository jobRepository, ItemReader<Movie> movieMySqlItemReader, ItemWriter<Movie> movieFlatFileItemWriter, PlatformTransactionManager transactionManager) {
        return new StepBuilder("movieExportStep", jobRepository)
                .<Movie, Movie>chunk(100)
                .reader(movieMySqlItemReader)
                .writer(movieFlatFileItemWriter)
                .transactionManager(transactionManager)
                .build();
    }


    @Bean
    public ItemReader<Movie> movieMySqlItemReader(DataSource dataSource) {
        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);
        return new JdbcPagingItemReaderBuilder<Movie>()
                .name("movieItemReader")
                .selectClause("select id, title, view_count, comment_count, like_count")
                .fromClause("from movie")
                .pageSize(100)
                .rowMapper((rs, rowNum) -> {
                    long id = rs.getLong("id");
                    String title = rs.getString("title");
                    long viewCount = rs.getLong("view_count");
                    long commentCount = rs.getLong("comment_count");
                    long likeCount = rs.getLong("like_count");
                    return new Movie(id, title, viewCount, commentCount, likeCount);
                })
                .sortKeys(sortKeys)
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public ItemWriter<Movie> movieFlatFileItemWriter() {
        BeanWrapperFieldExtractor<Movie> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"id", "title", "viewCount", "commentCount", "likeCount"});
        DelimitedLineAggregator<Movie> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(extractor);
        return new FlatFileItemWriterBuilder<Movie>()
                .name("movieItemWriter")
                .resource(new FileSystemResource("target/test-outputs/output.csv"))
                .lineAggregator(lineAggregator)
                .headerCallback(w -> w.write("id,title,viewCount,commentCount,likeCount"))
                .build();
    }
}
