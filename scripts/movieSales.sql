CREATE STREAM MOVIE_TICKET_SALES (title VARCHAR, sale_ts VARCHAR, ticket_total_value INT)
    WITH (KAFKA_TOPIC='movie-ticket-sales',
          PARTITIONS=1,
          VALUE_FORMAT='avro');

INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('Aliens', '2019-07-18T10:00:00Z', 10);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('Die Hard', '2019-07-18T10:00:00Z', 12);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('Die Hard', '2019-07-18T10:01:00Z', 12);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('The Godfather', '2019-07-18T10:01:31Z', 12);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('Die Hard', '2019-07-18T10:01:36Z', 24);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('The Godfather', '2019-07-18T10:02:00Z', 18);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('The Big Lebowski', '2019-07-18T11:03:21Z', 12);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('The Big Lebowski', '2019-07-18T11:03:50Z', 12);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('The Godfather', '2019-07-18T11:40:00Z', 36);
INSERT INTO MOVIE_TICKET_SALES (title, sale_ts, ticket_total_value) VALUES ('The Godfather', '2019-07-18T11:40:09Z', 18);


SET 'auto.offset.reset' = 'earliest';


SET 'ksql.streams.cache.max.bytes.buffering' = '10000000';


SELECT TITLE,
       COUNT(TICKET_TOTAL_VALUE) AS TICKETS_SOLD
FROM MOVIE_TICKET_SALES
GROUP BY TITLE
EMIT CHANGES
LIMIT 3;