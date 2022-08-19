// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

// use super::*;
// use datafusion::{
//     datasource::empty::EmptyTable, from_slice::FromSlice,
//     physical_plan::collect_partitioned,
// };
// use tempfile::TempDir;

// #[tokio::test]
// async fn all_where_empty() -> Result<()> {
//     let ctx = SessionContext::new();
//     register_aggregate_csv(&ctx).await?;
//     let sql = "SELECT *
//                FROM aggregate_test_100
//                WHERE 1=2";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec!["++", "++"];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

use std::sync::Arc;

use arrow::{
    array::{ListBuilder, PrimitiveBuilder},
    datatypes::{DataType, Field, Int64Type, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::MemTable,
    prelude::{CsvReadOptions, ParquetReadOptions, SessionContext},
};
use datafusion_common::Result;
use datafusion_sql::{
    parser::DFParser, planner::SqlToRel, sqlparser::ast::Statement as SQLStatement,
};

use datafusion_sqlgen::RelToSql;
use sqlgenerator::generate_sql;

async fn test_sql_roundtrip(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
    let state = ctx.state.as_ref().read().clone();
    let planner = SqlToRel::new(&state);
    let generator = RelToSql {};

    let ast = DFParser::parse_sql(sql)?;

    let plan = planner.statement_to_plan(ast[0].clone())?;
    let query = generator.plan_to_query(&plan, None)?;

    generate_sql(vec![SQLStatement::Query(Box::new(query))])
}

#[tokio::test]
async fn it_works() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "table1",
        "parquet-testing/data/alltypes_plain.parquet",
        ParquetReadOptions::default(),
    )
    .await?;
    let sql = "SELECT table1.id, table1.bool_col FROM table1\nWHERE table1.bool_col = TRUE;";

    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

#[tokio::test]
async fn select() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "aggregate_simple",
        "arrow-testing/data/csv/aggregate_test_100.csv",
        CsvReadOptions::default(),
    )
    .await?;

    let sql = "SELECT aggregate_simple.c1 FROM aggregate_simple\nORDER BY aggregate_simple.c1;";

    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

// #[tokio::test]
// async fn select_all() -> Result<()> {
//     let ctx = SessionContext::new();
//     ctx
//         .register_csv(
//             "aggregate_simple",
//             "arrow-testing/data/csv/aggregate_test_100.csv",
//             CsvReadOptions::default(),
//         )
//         .await?;

//     let sql = "SELECT ALL c1 FROM aggregate_simple order by c1";

//     let result = test_sql_roundtrip(&ctx, &sql).await?;

//     assert_eq!(sql, result[0]);
//     Ok(())
// }

#[tokio::test]
async fn select_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "aggregate_simple",
        "arrow-testing/data/csv/aggregate_test_100.csv",
        CsvReadOptions::default(),
    )
    .await?;

    let sql = "SELECT DISTINCT aggregate_simple.c1, aggregate_simple.c2, aggregate_simple.c3, aggregate_simple.c4, aggregate_simple.c5, aggregate_simple.c6, aggregate_simple.c7, aggregate_simple.c8, aggregate_simple.c9, aggregate_simple.c10, aggregate_simple.c11, aggregate_simple.c12, aggregate_simple.c13 FROM aggregate_simple;";

    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

#[tokio::test]
async fn select_distinct_simple_1() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "aggregate_simple",
        "arrow-testing/data/csv/aggregate_test_100.csv",
        CsvReadOptions::default(),
    )
    .await?;

    let sql =
        "SELECT DISTINCT aggregate_simple.c1 FROM aggregate_simple\nORDER BY aggregate_simple.c1;";

    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

#[tokio::test]
async fn select_distinct_simple_2() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "aggregate_simple",
        "arrow-testing/data/csv/aggregate_test_100.csv",
        CsvReadOptions::default(),
    )
    .await?;

    let sql =
        "SELECT DISTINCT aggregate_simple.c1, aggregate_simple.c2 FROM aggregate_simple\nORDER BY aggregate_simple.c1;";

    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

#[tokio::test]
async fn select_distinct_simple_4() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "aggregate_simple",
        "arrow-testing/data/csv/aggregate_test_100.csv",
        CsvReadOptions::default(),
    )
    .await?;

    let sql = "SELECT DISTINCT CAST(aggregate_simple.c1 AS INT) + aggregate_simple.c2 AS a FROM aggregate_simple;";

    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

#[tokio::test]
async fn select_distinct_from() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "aggregate_simple",
        "arrow-testing/data/csv/aggregate_test_100.csv",
        CsvReadOptions::default(),
    )
    .await?;

    let sql = "SELECT 1 IS DISTINCT FROM CAST(NULL AS INT) AS a, 1 IS DISTINCT FROM 1 AS b, 1 IS NOT DISTINCT FROM CAST(NULL AS INT) AS c, 1 IS NOT DISTINCT FROM 1 AS d, NULL IS DISTINCT FROM NULL AS e, NULL IS NOT DISTINCT FROM NULL AS f, NULL IS DISTINCT FROM 1 AS g, NULL IS NOT DISTINCT FROM 1 AS h ;";

    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

#[tokio::test]
async fn select_distinct_from_utf8() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "aggregate_simple",
        "arrow-testing/data/csv/aggregate_test_100.csv",
        CsvReadOptions::default(),
    )
    .await?;

    let sql = "SELECT 'x' IS DISTINCT FROM NULL AS a, 'x' IS DISTINCT FROM 'x' AS b, 'x' IS NOT DISTINCT FROM NULL AS c, 'x' IS NOT DISTINCT FROM 'x' AS d ;";
    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

#[tokio::test]
async fn use_between_expression_in_select_query() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "aggregate_simple",
        "arrow-testing/data/csv/aggregate_test_100.csv",
        CsvReadOptions::default(),
    )
    .await?;

    let sql = "SELECT abs(aggregate_simple.c1) BETWEEN 0 AND log(aggregate_simple.c1 * 100) FROM aggregate_simple;";

    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

#[tokio::test]
async fn query_get_indexed_field() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_list",
        DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
        false,
    )]));
    let builder = PrimitiveBuilder::<Int64Type>::new(3);
    let mut lb = ListBuilder::new(builder);
    for int_vec in vec![vec![0, 1, 2], vec![4, 5, 6], vec![7, 8, 9]] {
        let builder = lb.values();
        for int in int_vec {
            builder.append_value(int).unwrap();
        }
        lb.append(true).unwrap();
    }

    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    let table_a = Arc::new(table);

    ctx.register_table("ints", table_a)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT ints.some_list[1] AS i0 FROM ints\nLIMIT 3;";
    let result = test_sql_roundtrip(&ctx, &sql).await?;

    assert_eq!(sql, result[0]);
    Ok(())
}

// #[tokio::test]
// async fn query_get_indexed_field() -> Result<()> {
//     let ctx = SessionContext::new();
//     let schema = Arc::new(Schema::new(vec![Field::new(
//         "some_list",
//         DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
//         false,
//     )]));
//     let builder = PrimitiveBuilder::<Int64Type>::new(3);
//     let mut lb = ListBuilder::new(builder);
//     for int_vec in vec![vec![0, 1, 2], vec![4, 5, 6], vec![7, 8, 9]] {
//         let builder = lb.values();
//         for int in int_vec {
//             builder.append_value(int);
//         }
//         lb.append(true);
//     }

//     let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;
//     let table = MemTable::try_new(schema, vec![vec![data]])?;
//     let table_a = Arc::new(table);

//     ctx.register_table("ints", table_a)?;

//     // Original column is micros, convert to millis and check timestamp
//     let sql = "SELECT some_list[1] as i0 FROM ints LIMIT 3";
//     let actual = execute_to_batches(&ctx, sql).await;
//     #[rustfmt::skip]
//     let expected = vec![
//         "+----+",
//         "| i0 |",
//         "+----+",
//         "| 0  |",
//         "| 4  |",
//         "| 7  |",
//         "+----+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

// #[tokio::test]
// async fn query_nested_get_indexed_field() -> Result<()> {
//     let ctx = SessionContext::new();
//     let nested_dt = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
//     // Nested schema of { "some_list": [[i64]] }
//     let schema = Arc::new(Schema::new(vec![Field::new(
//         "some_list",
//         DataType::List(Box::new(Field::new("item", nested_dt.clone(), true))),
//         false,
//     )]));

//     let builder = PrimitiveBuilder::<Int64Type>::new(3);
//     let nested_lb = ListBuilder::new(builder);
//     let mut lb = ListBuilder::new(nested_lb);
//     for int_vec_vec in vec![
//         vec![vec![0, 1], vec![2, 3], vec![3, 4]],
//         vec![vec![5, 6], vec![7, 8], vec![9, 10]],
//         vec![vec![11, 12], vec![13, 14], vec![15, 16]],
//     ] {
//         let nested_builder = lb.values();
//         for int_vec in int_vec_vec {
//             let builder = nested_builder.values();
//             for int in int_vec {
//                 builder.append_value(int);
//             }
//             nested_builder.append(true);
//         }
//         lb.append(true);
//     }

//     let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;
//     let table = MemTable::try_new(schema, vec![vec![data]])?;
//     let table_a = Arc::new(table);

//     ctx.register_table("ints", table_a)?;

//     // Original column is micros, convert to millis and check timestamp
//     let sql = "SELECT some_list[1] as i0 FROM ints LIMIT 3";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----------+",
//         "| i0       |",
//         "+----------+",
//         "| [0, 1]   |",
//         "| [5, 6]   |",
//         "| [11, 12] |",
//         "+----------+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     let sql = "SELECT some_list[1][1] as i0 FROM ints LIMIT 3";
//     let actual = execute_to_batches(&ctx, sql).await;
//     #[rustfmt::skip]
//     let expected = vec![
//         "+----+",
//         "| i0 |",
//         "+----+",
//         "| 0  |",
//         "| 5  |",
//         "| 11 |",
//         "+----+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

// #[tokio::test]
// async fn query_nested_get_indexed_field_on_struct() -> Result<()> {
//     let ctx = SessionContext::new();
//     let nested_dt = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
//     // Nested schema of { "some_struct": { "bar": [i64] } }
//     let struct_fields = vec![Field::new("bar", nested_dt.clone(), true)];
//     let schema = Arc::new(Schema::new(vec![Field::new(
//         "some_struct",
//         DataType::Struct(struct_fields.clone()),
//         false,
//     )]));

//     let builder = PrimitiveBuilder::<Int64Type>::new(3);
//     let nested_lb = ListBuilder::new(builder);
//     let mut sb = StructBuilder::new(struct_fields, vec![Box::new(nested_lb)]);
//     for int_vec in vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]] {
//         let lb = sb.field_builder::<ListBuilder<Int64Builder>>(0).unwrap();
//         for int in int_vec {
//             lb.values().append_value(int);
//         }
//         lb.append(true);
//     }
//     let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(sb.finish())])?;
//     let table = MemTable::try_new(schema, vec![vec![data]])?;
//     let table_a = Arc::new(table);

//     ctx.register_table("structs", table_a)?;

//     // Original column is micros, convert to millis and check timestamp
//     let sql = "SELECT some_struct['bar'] as l0 FROM structs LIMIT 3";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----------------+",
//         "| l0             |",
//         "+----------------+",
//         "| [0, 1, 2, 3]   |",
//         "| [4, 5, 6, 7]   |",
//         "| [8, 9, 10, 11] |",
//         "+----------------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // Access to field of struct by CompoundIdentifier
//     let sql = "SELECT some_struct.bar as l0 FROM structs LIMIT 3";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----------------+",
//         "| l0             |",
//         "+----------------+",
//         "| [0, 1, 2, 3]   |",
//         "| [4, 5, 6, 7]   |",
//         "| [8, 9, 10, 11] |",
//         "+----------------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     let sql = "SELECT some_struct['bar'][1] as i0 FROM structs LIMIT 3";
//     let actual = execute_to_batches(&ctx, sql).await;
//     #[rustfmt::skip]
//     let expected = vec![
//         "+----+",
//         "| i0 |",
//         "+----+",
//         "| 0  |",
//         "| 4  |",
//         "| 8  |",
//         "+----+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

// #[tokio::test]
// async fn query_on_string_dictionary() -> Result<()> {
//     // Test to ensure DataFusion can operate on dictionary types
//     // Use StringDictionary (32 bit indexes = keys)
//     let d1: DictionaryArray<Int32Type> =
//         vec![Some("one"), None, Some("three")].into_iter().collect();

//     let d2: DictionaryArray<Int32Type> = vec![Some("blarg"), None, Some("three")]
//         .into_iter()
//         .collect();

//     let d3: StringArray = vec![Some("XYZ"), None, Some("three")].into_iter().collect();

//     let batch = RecordBatch::try_from_iter(vec![
//         ("d1", Arc::new(d1) as ArrayRef),
//         ("d2", Arc::new(d2) as ArrayRef),
//         ("d3", Arc::new(d3) as ArrayRef),
//     ])
//     .unwrap();

//     let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
//     let ctx = SessionContext::new();
//     ctx.register_table("test", Arc::new(table))?;

//     // Basic SELECT
//     let sql = "SELECT d1 FROM test";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+",
//         "| d1    |",
//         "+-------+",
//         "| one   |",
//         "|       |",
//         "| three |",
//         "+-------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // basic filtering
//     let sql = "SELECT d1 FROM test WHERE d1 IS NOT NULL";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+",
//         "| d1    |",
//         "+-------+",
//         "| one   |",
//         "| three |",
//         "+-------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // comparison with constant
//     let sql = "SELECT d1 FROM test WHERE d1 = 'three'";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+",
//         "| d1    |",
//         "+-------+",
//         "| three |",
//         "+-------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // comparison with another dictionary column
//     let sql = "SELECT d1 FROM test WHERE d1 = d2";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+",
//         "| d1    |",
//         "+-------+",
//         "| three |",
//         "+-------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // order comparison with another dictionary column
//     let sql = "SELECT d1 FROM test WHERE d1 <= d2";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+",
//         "| d1    |",
//         "+-------+",
//         "| three |",
//         "+-------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // comparison with a non dictionary column
//     let sql = "SELECT d1 FROM test WHERE d1 = d3";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+",
//         "| d1    |",
//         "+-------+",
//         "| three |",
//         "+-------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // filtering with constant
//     let sql = "SELECT d1 FROM test WHERE d1 = 'three'";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+",
//         "| d1    |",
//         "+-------+",
//         "| three |",
//         "+-------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // Expression evaluation
//     let sql = "SELECT concat(d1, '-foo') FROM test";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+------------------------------+",
//         "| concat(test.d1,Utf8(\"-foo\")) |",
//         "+------------------------------+",
//         "| one-foo                      |",
//         "| -foo                         |",
//         "| three-foo                    |",
//         "+------------------------------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // Expression evaluation with two dictionaries
//     let sql = "SELECT concat(d1, d2) FROM test";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------------------------+",
//         "| concat(test.d1,test.d2) |",
//         "+-------------------------+",
//         "| oneblarg                |",
//         "|                         |",
//         "| threethree              |",
//         "+-------------------------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // aggregation
//     let sql = "SELECT COUNT(d1) FROM test";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----------------+",
//         "| COUNT(test.d1) |",
//         "+----------------+",
//         "| 2              |",
//         "+----------------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // aggregation min
//     let sql = "SELECT MIN(d1) FROM test";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+--------------+",
//         "| MIN(test.d1) |",
//         "+--------------+",
//         "| one          |",
//         "+--------------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // aggregation max
//     let sql = "SELECT MAX(d1) FROM test";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+--------------+",
//         "| MAX(test.d1) |",
//         "+--------------+",
//         "| three        |",
//         "+--------------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // grouping
//     let sql = "SELECT d1, COUNT(*) FROM test group by d1";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+-----------------+",
//         "| d1    | COUNT(UInt8(1)) |",
//         "+-------+-----------------+",
//         "| one   | 1               |",
//         "|       | 1               |",
//         "| three | 1               |",
//         "+-------+-----------------+",
//     ];
//     assert_batches_sorted_eq!(expected, &actual);

//     // window functions
//     let sql = "SELECT d1, row_number() OVER (partition by d1) FROM test";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+-------+--------------+",
//         "| d1    | ROW_NUMBER() |",
//         "+-------+--------------+",
//         "|       | 1            |",
//         "| one   | 1            |",
//         "| three | 1            |",
//         "+-------+--------------+",
//     ];
//     assert_batches_sorted_eq!(expected, &actual);

//     Ok(())
// }

// #[tokio::test]
// async fn query_cte_with_alias() -> Result<()> {
//     let ctx = SessionContext::new();
//     let schema = Schema::new(vec![
//         Field::new("id", DataType::Int16, false),
//         Field::new("a", DataType::Int16, false),
//     ]);
//     let empty_table = Arc::new(EmptyTable::new(Arc::new(schema)));
//     ctx.register_table("t1", empty_table)?;
//     let sql = "WITH \
//         v1 AS (SELECT * FROM t1), \
//         v2 AS (SELECT v1.id AS id, v1a.id AS id_a, v1b.id AS id_b \
//         FROM v1, v1 v1a, v1 v1b \
//         WHERE v1a.id = v1.id - 1 \
//         AND v1b.id = v1.id + 1) \
//         SELECT * FROM v2";
//     let actual = execute_to_batches(&ctx, sql).await;
//     // the purpose of this test is just to make sure the query produces a valid plan
//     let expected = vec!["++", "++"];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

// #[tokio::test]
// async fn query_cte() -> Result<()> {
//     // Test for SELECT <expression> without FROM.
//     // Should evaluate expressions in project position.
//     let ctx = SessionContext::new();

//     // simple with
//     let sql = "WITH t AS (SELECT 1) SELECT * FROM t";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----------+",
//         "| Int64(1) |",
//         "+----------+",
//         "| 1        |",
//         "+----------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     // with + union
//     let sql =
//         "WITH t AS (SELECT 1 AS a), u AS (SELECT 2 AS a) SELECT * FROM t UNION ALL SELECT * FROM u";
//     let actual = execute_to_batches(&ctx, sql).await;
//     #[rustfmt::skip]
//     let expected = vec![
//         "+---+",
//         "| a |",
//         "+---+",
//         "| 1 |",
//         "| 2 |",
//         "+---+"
//     ];
//     assert_batches_eq!(expected, &actual);

//     // with + join
//     let sql = "WITH t AS (SELECT 1 AS id1), u AS (SELECT 1 AS id2, 5 as x) SELECT x FROM t JOIN u ON (id1 = id2)";
//     let actual = execute_to_batches(&ctx, sql).await;
//     #[rustfmt::skip]
//     let expected = vec![
//         "+---+",
//         "| x |",
//         "+---+",
//         "| 5 |",
//         "+---+"
//     ];
//     assert_batches_eq!(expected, &actual);

//     // backward reference
//     let sql = "WITH t AS (SELECT 1 AS id1), u AS (SELECT * FROM t) SELECT * from u";
//     let actual = execute_to_batches(&ctx, sql).await;
//     #[rustfmt::skip]
//     let expected = vec![
//         "+-----+",
//         "| id1 |",
//         "+-----+",
//         "| 1   |",
//         "+-----+"
//     ];
//     assert_batches_eq!(expected, &actual);

//     Ok(())
// }

// #[tokio::test]
// async fn csv_select_nested() -> Result<()> {
//     let ctx = SessionContext::new();
//     register_aggregate_csv(&ctx).await?;
//     let sql = "SELECT o1, o2, c3
//                FROM (
//                  SELECT c1 AS o1, c2 + 1 AS o2, c3
//                  FROM (
//                    SELECT c1, c2, c3, c4
//                    FROM aggregate_test_100
//                    WHERE c1 = 'a' AND c2 >= 4
//                    ORDER BY c2 ASC, c3 ASC
//                  ) AS a
//                ) AS b";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----+----+------+",
//         "| o1 | o2 | c3   |",
//         "+----+----+------+",
//         "| a  | 5  | -101 |",
//         "| a  | 5  | -54  |",
//         "| a  | 5  | -38  |",
//         "| a  | 5  | 65   |",
//         "| a  | 6  | -101 |",
//         "| a  | 6  | -31  |",
//         "| a  | 6  | 36   |",
//         "+----+----+------+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

// #[tokio::test]
// async fn csv_select_nested_without_aliases() -> Result<()> {
//     let ctx = SessionContext::new();
//     register_aggregate_csv(&ctx).await?;
//     let sql = "SELECT o1, o2, c3
//                FROM (
//                  SELECT c1 AS o1, c2 + 1 AS o2, c3
//                  FROM (
//                    SELECT c1, c2, c3, c4
//                    FROM aggregate_test_100
//                    WHERE c1 = 'a' AND c2 >= 4
//                    ORDER BY c2 ASC, c3 ASC
//                  )
//                )";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----+----+------+",
//         "| o1 | o2 | c3   |",
//         "+----+----+------+",
//         "| a  | 5  | -101 |",
//         "| a  | 5  | -54  |",
//         "| a  | 5  | -38  |",
//         "| a  | 5  | 65   |",
//         "| a  | 6  | -101 |",
//         "| a  | 6  | -31  |",
//         "| a  | 6  | 36   |",
//         "+----+----+------+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

// #[tokio::test]
// async fn csv_join_unaliased_subqueries() -> Result<()> {
//     let ctx = SessionContext::new();
//     register_aggregate_csv(&ctx).await?;
//     let sql = "SELECT o1, o2, c3, p1, p2, p3 FROM \
//         (SELECT c1 AS o1, c2 + 1 AS o2, c3 FROM aggregate_test_100), \
//         (SELECT c1 AS p1, c2 - 1 AS p2, c3 AS p3 FROM aggregate_test_100) LIMIT 5";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----+----+----+----+----+-----+",
//         "| o1 | o2 | c3 | p1 | p2 | p3  |",
//         "+----+----+----+----+----+-----+",
//         "| c  | 3  | 1  | c  | 1  | 1   |",
//         "| c  | 3  | 1  | d  | 4  | -40 |",
//         "| c  | 3  | 1  | b  | 0  | 29  |",
//         "| c  | 3  | 1  | a  | 0  | -85 |",
//         "| c  | 3  | 1  | b  | 4  | -82 |",
//         "+----+----+----+----+----+-----+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

// #[tokio::test]
// async fn parallel_query_with_filter() -> Result<()> {
//     let tmp_dir = TempDir::new()?;
//     let partition_count = 4;
//     let ctx = partitioned_csv::create_ctx(&tmp_dir, partition_count).await?;

//     let logical_plan =
//         ctx.create_logical_plan("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")?;
//     let logical_plan = ctx.optimize(&logical_plan)?;

//     let physical_plan = ctx.create_physical_plan(&logical_plan).await?;

//     let task_ctx = ctx.task_ctx();
//     let results = collect_partitioned(physical_plan, task_ctx).await?;

//     // note that the order of partitions is not deterministic
//     let mut num_rows = 0;
//     for partition in &results {
//         for batch in partition {
//             num_rows += batch.num_rows();
//         }
//     }
//     assert_eq!(20, num_rows);

//     let results: Vec<RecordBatch> = results.into_iter().flatten().collect();
//     let expected = vec![
//         "+----+----+",
//         "| c1 | c2 |",
//         "+----+----+",
//         "| 1  | 1  |",
//         "| 1  | 10 |",
//         "| 1  | 2  |",
//         "| 1  | 3  |",
//         "| 1  | 4  |",
//         "| 1  | 5  |",
//         "| 1  | 6  |",
//         "| 1  | 7  |",
//         "| 1  | 8  |",
//         "| 1  | 9  |",
//         "| 2  | 1  |",
//         "| 2  | 10 |",
//         "| 2  | 2  |",
//         "| 2  | 3  |",
//         "| 2  | 4  |",
//         "| 2  | 5  |",
//         "| 2  | 6  |",
//         "| 2  | 7  |",
//         "| 2  | 8  |",
//         "| 2  | 9  |",
//         "+----+----+",
//     ];
//     assert_batches_sorted_eq!(expected, &results);

//     Ok(())
// }

// #[tokio::test]
// async fn query_with_filter_string_type_coercion() {
//     let large_string_array = LargeStringArray::from(vec!["1", "2", "3", "4", "5"]);
//     let schema =
//         Schema::new(vec![Field::new("large_string", DataType::LargeUtf8, false)]);
//     let batch =
//         RecordBatch::try_new(Arc::new(schema), vec![Arc::new(large_string_array)])
//             .unwrap();

//     let ctx = SessionContext::new();
//     let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
//     ctx.register_table("t", Arc::new(table)).unwrap();
//     let sql = "select * from t where large_string = '1'";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+--------------+",
//         "| large_string |",
//         "+--------------+",
//         "| 1            |",
//         "+--------------+",
//     ];
//     assert_batches_eq!(expected, &actual);

//     let sql = "select * from t where large_string != '1'";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+--------------+",
//         "| large_string |",
//         "+--------------+",
//         "| 2            |",
//         "| 3            |",
//         "| 4            |",
//         "| 5            |",
//         "+--------------+",
//     ];
//     assert_batches_eq!(expected, &actual);
// }

// #[tokio::test]
// async fn query_empty_table() {
//     let ctx = SessionContext::new();
//     let empty_table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
//     ctx.register_table("test_tbl", empty_table).unwrap();
//     let sql = "SELECT * FROM test_tbl";
//     let result = plan_and_collect(&ctx, sql)
//         .await
//         .expect("Query empty table");
//     let expected = vec!["++", "++"];
//     assert_batches_sorted_eq!(expected, &result);
// }

// #[tokio::test]
// async fn boolean_literal() -> Result<()> {
//     let results =
//         execute_with_partition("SELECT c1, c3 FROM test WHERE c1 > 2 AND c3 = true", 4)
//             .await?;

//     let expected = vec![
//         "+----+------+",
//         "| c1 | c3   |",
//         "+----+------+",
//         "| 3  | true |",
//         "| 3  | true |",
//         "| 3  | true |",
//         "| 3  | true |",
//         "| 3  | true |",
//         "+----+------+",
//     ];
//     assert_batches_sorted_eq!(expected, &results);

//     Ok(())
// }

// #[tokio::test]
// async fn unprojected_filter() {
//     let ctx = SessionContext::new();
//     let df = ctx.read_table(table_with_sequence(1, 3).unwrap()).unwrap();

//     let df = df
//         .select(vec![col("i") + col("i")])
//         .unwrap()
//         .filter(col("i").gt(lit(2)))
//         .unwrap();
//     let results = df.collect().await.unwrap();

//     let expected = vec![
//         "+--------------------------+",
//         "| ?table?.i Plus ?table?.i |",
//         "+--------------------------+",
//         "| 6                        |",
//         "+--------------------------+",
//     ];
//     assert_batches_sorted_eq!(expected, &results);
// }
