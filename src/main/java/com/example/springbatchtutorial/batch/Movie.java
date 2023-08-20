package com.example.springbatchtutorial.batch;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Movie {
    private Long id;
    private String title;
    private Long viewCount;
    private Long commentCount;
    private Long likeCount;
}
