select format('refresh materialized view %s.%s', schemaname, matviewname) from pg_matviews;
select format('vacuum analyze %s.%s', schemaname, matviewname) from pg_matviews;
