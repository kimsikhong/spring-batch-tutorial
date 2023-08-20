package com.example.springbatchtutorial.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@Slf4j
public class MovieImportConfiguration {

    @Bean
    public Job movieJob(JobRepository jobRepository, Step movieStep) {
        return new JobBuilder("movieJob", jobRepository)
                .start(movieStep)
                .build();
    }

    @Bean
    public Step movieStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, ItemWriter<Movie> mysqlMovieItemWriter) {
        return new StepBuilder("movieStep", jobRepository)
                .<Movie, Movie>chunk(10, transactionManager)
                .reader(movieItemReader())
//                .writer(noopMovieItemWriter())
                .writer(mysqlMovieItemWriter)
                .build();
    }

    @Bean
    public ItemReader<Movie> movieItemReader() {
        FlatFileItemReader<Movie> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new ClassPathResource("MOCK_DATA.csv"));
        DefaultLineMapper<Movie> lineMapper = new DefaultLineMapper<>();
        //DelimitedLineTokenizer defaults to comma as its delimiter
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "title", "viewCount", "commentCount", "likeCount");
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new MovieFieldSetMapper());
        itemReader.setLineMapper(lineMapper);
        itemReader.setLinesToSkip(1);
        return itemReader;
    }

    @Bean
    public ItemWriter<Movie> noopMovieItemWriter() {
        return chunk -> {
            for (var movie : chunk.getItems()) {
                log.info("write movie : {}", movie);
            }
        };
    }

    @Bean
    public ItemWriter<Movie> mysqlMovieItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Movie>()
                .sql("insert into movie(id, title, view_count, comment_count, like_count) values (:id, :title, :viewCount, :commentCount, :likeCount)")
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    protected static class MovieFieldSetMapper implements FieldSetMapper<Movie> {
        public Movie mapFieldSet(FieldSet fieldSet) {
            Long id = fieldSet.readLong("id");
            String title = fieldSet.readString("title");
            Long viewCount = fieldSet.readLong("viewCount");
            Long commentCount = fieldSet.readLong("commentCount");
            Long likeCount = fieldSet.readLong("likeCount");
            return new Movie(id, title, viewCount, commentCount, likeCount);
        }
    }
}
