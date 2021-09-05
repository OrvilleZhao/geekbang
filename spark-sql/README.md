#作业1:为Spark SQL添加一条自定义命令
1.SqlBase.g4添加相关参数
<img src="img/img2.jpg" />

2.通过antlr4编译出SqlBaseParser.java

3.在sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala中添加
```
  override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
     ShowVersionCommand()
  }
```

4.添加ShowVersionCommand
```
case class ShowVersionCommand() extends LeafRunnableCommand {
  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val javaVersion = System.getProperty("java.version")
    val outputString = "Spark Version:3.3.0; Java Version:" + javaVersion + ";"
    Seq(Row(outputString))
  } catch { case NonFatal(cause) =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}
```

5.运行结果
<img src="img/img1.jpg"/>

# 作业2：
SQL1：
```
select name from (select id, name from test a where id >1 and 1 = 1 ) where name='tom' and id <5;
```
```
spark-sql> select name from (select id, name from test a where id >1 and 1 = 1 ) where name='tom' and id <5;
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Substitution has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Disable Hints has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Hints has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Simple Sanity Check has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations ===
 'Project ['name]                                          'Project ['name]
 +- 'Filter (('name = tom) AND ('id < 5))                  +- 'Filter (('name = tom) AND ('id < 5))
    +- 'SubqueryAlias __auto_generated_subquery_name          +- 'SubqueryAlias __auto_generated_subquery_name
       +- 'Project ['id, 'name]                                  +- 'Project ['id, 'name]
          +- 'Filter (('id > 1) AND (1 = 1))                        +- 'Filter (('id > 1) AND (1 = 1))
             +- 'SubqueryAlias a                                       +- 'SubqueryAlias a
!               +- 'UnresolvedRelation [test], [], false                  +- 'SubqueryAlias spark_catalog.default.test
!                                                                            +- 'UnresolvedCatalogRelation `default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [], false

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.execution.datasources.FindDataSourceTable ===
 'Project ['name]                                                                                                                  'Project ['name]
 +- 'Filter (('name = tom) AND ('id < 5))                                                                                          +- 'Filter (('name = tom) AND ('id < 5))
    +- 'SubqueryAlias __auto_generated_subquery_name                                                                                  +- 'SubqueryAlias __auto_generated_subquery_name
       +- 'Project ['id, 'name]                                                                                                          +- 'Project ['id, 'name]
          +- 'Filter (('id > 1) AND (1 = 1))                                                                                                +- 'Filter (('id > 1) AND (1 = 1))
!            +- 'SubqueryAlias a                                                                                                               +- SubqueryAlias a
!               +- 'SubqueryAlias spark_catalog.default.test                                                                                      +- SubqueryAlias spark_catalog.default.test
!                  +- 'UnresolvedCatalogRelation `default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [], false                     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveReferences ===
!'Project ['name]                                                                                                                                                            Project [name#264]
!+- 'Filter (('name = tom) AND ('id < 5))                                                                                                                                    +- Filter ((name#264 = tom) AND (id#263 < 5))
!   +- 'SubqueryAlias __auto_generated_subquery_name                                                                                                                            +- SubqueryAlias __auto_generated_subquery_name
!      +- 'Project ['id, 'name]                                                                                                                                                    +- Project [id#263, name#264]
!         +- 'Filter (('id > 1) AND (1 = 1))                                                                                                                                          +- Filter ((id#263 > 1) AND (1 = 1))
             +- SubqueryAlias a                                                                                                                                                          +- SubqueryAlias a
                +- SubqueryAlias spark_catalog.default.test                                                                                                                                 +- SubqueryAlias spark_catalog.default.test
                   +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]                     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Result of Batch Resolution ===
!'Project ['name]                                          Project [name#264]
!+- 'Filter (('name = tom) AND ('id < 5))                  +- Filter ((name#264 = tom) AND (id#263 < 5))
!   +- 'SubqueryAlias __auto_generated_subquery_name          +- SubqueryAlias __auto_generated_subquery_name
!      +- 'Project ['id, 'name]                                  +- Project [id#263, name#264]
!         +- 'Filter (('id > 1) AND (1 = 1))                        +- Filter ((id#263 > 1) AND (1 = 1))
!            +- 'SubqueryAlias a                                       +- SubqueryAlias a
!               +- 'UnresolvedRelation [test], [], false                  +- SubqueryAlias spark_catalog.default.test
!                                                                            +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger: Batch Remove TempResolvedColumn has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Apply Char Padding has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Post-Hoc Resolution has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Remove Unresolved Hints has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Nondeterministic has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch UDF has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch UpdateNullability has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Subquery has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Cleanup has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch HandleAnalysisOnlyCommand has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 186
Total time: 0.053965134 seconds
Total number of effective runs: 3
Total time of effective runs: 0.045662922 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch Eliminate Distinct has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases ===
 Project [name#264]                                                                                                                                                          Project [name#264]
 +- Filter ((name#264 = tom) AND (id#263 < 5))                                                                                                                               +- Filter ((name#264 = tom) AND (id#263 < 5))
!   +- SubqueryAlias __auto_generated_subquery_name                                                                                                                             +- Project [id#263, name#264]
!      +- Project [id#263, name#264]                                                                                                                                               +- Filter ((id#263 > 1) AND (1 = 1))
!         +- Filter ((id#263 > 1) AND (1 = 1))                                                                                                                                        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]
!            +- SubqueryAlias a
!               +- SubqueryAlias spark_catalog.default.test
!                  +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Result of Batch Finish Analysis ===
 Project [name#264]                                                                                                                                                          Project [name#264]
 +- Filter ((name#264 = tom) AND (id#263 < 5))                                                                                                                               +- Filter ((name#264 = tom) AND (id#263 < 5))
!   +- SubqueryAlias __auto_generated_subquery_name                                                                                                                             +- Project [id#263, name#264]
!      +- Project [id#263, name#264]                                                                                                                                               +- Filter ((id#263 > 1) AND (1 = 1))
!         +- Filter ((id#263 > 1) AND (1 = 1))                                                                                                                                        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]
!            +- SubqueryAlias a
!               +- SubqueryAlias spark_catalog.default.test
!                  +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger: Batch Union has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch OptimizeLimitZero has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch LocalRelation early has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Pullup Correlated Expressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Subquery has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Replace Operators has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Aggregate has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Project [name#264]                                                                                                                                                 Project [name#264]
!+- Filter ((name#264 = tom) AND (id#263 < 5))                                                                                                                      +- Project [id#263, name#264]
!   +- Project [id#263, name#264]                                                                                                                                      +- Filter (((id#263 > 1) AND (1 = 1)) AND ((name#264 = tom) AND (id#263 < 5)))
!      +- Filter ((id#263 > 1) AND (1 = 1))                                                                                                                               +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]
!         +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
 Project [name#264]                                                                                                                                              Project [name#264]
!+- Project [id#263, name#264]                                                                                                                                   +- Project [name#264]
    +- Filter (((id#263 > 1) AND (1 = 1)) AND ((name#264 = tom) AND (id#263 < 5)))                                                                                  +- Filter (((id#263 > 1) AND (1 = 1)) AND ((name#264 = tom) AND (id#263 < 5)))
       +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]         +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Project [name#264]                                                                                                                                              Project [name#264]
!+- Project [name#264]                                                                                                                                           +- Filter (((id#263 > 1) AND (1 = 1)) AND ((name#264 = tom) AND (id#263 < 5)))
!   +- Filter (((id#263 > 1) AND (1 = 1)) AND ((name#264 = tom) AND (id#263 < 5)))                                                                                  +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]
!      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Project [name#264]                                                                                                                                           Project [name#264]
!+- Filter (((id#263 > 1) AND (1 = 1)) AND ((name#264 = tom) AND (id#263 < 5)))                                                                               +- Filter (((id#263 > 1) AND true) AND ((name#264 = tom) AND (id#263 < 5)))
    +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 Project [name#264]                                                                                                                                           Project [name#264]
!+- Filter (((id#263 > 1) AND true) AND ((name#264 = tom) AND (id#263 < 5)))                                                                                  +- Filter ((id#263 > 1) AND ((name#264 = tom) AND (id#263 < 5)))
    +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Result of Batch Operator Optimization before Inferring Filters ===
 Project [name#264]                                                                                                                                                 Project [name#264]
!+- Filter ((name#264 = tom) AND (id#263 < 5))                                                                                                                      +- Filter ((id#263 > 1) AND ((name#264 = tom) AND (id#263 < 5)))
!   +- Project [id#263, name#264]                                                                                                                                      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]
!      +- Filter ((id#263 > 1) AND (1 = 1))
!         +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints ===
 Project [name#264]                                                                                                                                           Project [name#264]
!+- Filter ((id#263 > 1) AND ((name#264 = tom) AND (id#263 < 5)))                                                                                             +- Filter ((isnotnull(id#263) AND isnotnull(name#264)) AND ((id#263 > 1) AND ((name#264 = tom) AND (id#263 < 5))))
    +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Result of Batch Infer Filters ===
 Project [name#264]                                                                                                                                           Project [name#264]
!+- Filter ((id#263 > 1) AND ((name#264 = tom) AND (id#263 < 5)))                                                                                             +- Filter ((isnotnull(id#263) AND isnotnull(name#264)) AND ((id#263 > 1) AND ((name#264 = tom) AND (id#263 < 5))))
    +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger: Batch Operator Optimization after Inferring Filters has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Push extra predicate through join has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Early Filter and Projection Push-Down has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Update CTE Relation Stats has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Join Reorder has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Eliminate Sorts has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Decimal Optimizations has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Distinct Aggregate Rewrite has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Object Expressions Optimization has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch LocalRelation has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Check Cartesian Products has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch RewriteSubquery has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch NormalizeFloatingNumbers has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch ReplaceUpdateFieldsExpression has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Optimize Metadata Only Query has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch PartitionPruning has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Pushdown Filters from PartitionPruning has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Cleanup filters that cannot be pushed down has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch Extract Python UDFs has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger: Batch User Provided Optimizers has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 213
Total time: 0.012860982 seconds
Total number of effective runs: 7
Total time of effective runs: 0.006089931 seconds

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.execution.CollapseCodegenStages ===
!Project [name#264]                                                                                                                                                                                      *(1) Project [name#264]
!+- Filter ((((isnotnull(id#263) AND isnotnull(name#264)) AND (id#263 > 1)) AND (name#264 = tom)) AND (id#263 < 5))                                                                                      +- *(1) Filter ((((isnotnull(id#263) AND isnotnull(name#264)) AND (id#263 > 1)) AND (name#264 = tom)) AND (id#263 < 5))
    +- Scan hive default.test [id#263, name#264], HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]      +- Scan hive default.test [id#263, name#264], HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger:
=== Result of Batch Preparations ===
!Project [name#264]                                                                                                                                                                                      *(1) Project [name#264]
!+- Filter ((((isnotnull(id#263) AND isnotnull(name#264)) AND (id#263 > 1)) AND (name#264 = tom)) AND (id#263 < 5))                                                                                      +- *(1) Filter ((((isnotnull(id#263) AND isnotnull(name#264)) AND (id#263 > 1)) AND (name#264 = tom)) AND (id#263 < 5))
    +- Scan hive default.test [id#263, name#264], HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]      +- Scan hive default.test [id#263, name#264], HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#263, name#264, tel#265], Partition Cols: []]

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 1.2047E-5 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 8.318E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 8.353E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 7.141E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 7.495E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 1.1699E-5 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 7.828E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 8.717E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 8.292E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 5.512E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 8.499E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 4.808E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 8.36E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 6.275E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 7.926E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

21/09/06 00:07:16 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
21/09/06 00:07:16 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 5.834E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds

tom
Time taken: 0.4 seconds, Fetched 1 row(s)
```
SQL2
```
select id,name, 'yes' as t from (select id,name from test where 1=1  except select id,name from test where id in (1,2) ) where name = 'tom' order by t;
```

```
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Substitution has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Disable Hints has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Hints has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Simple Sanity Check has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations ===
 'Sort ['t ASC NULLS FIRST], true                             'Sort ['t ASC NULLS FIRST], true
 +- 'Project ['id, 'name, yes AS t#34]                        +- 'Project ['id, 'name, yes AS t#34]
    +- 'Filter ('name = tom)                                     +- 'Filter ('name = tom)
       +- 'SubqueryAlias __auto_generated_subquery_name             +- 'SubqueryAlias __auto_generated_subquery_name
          +- 'Except false                                             +- 'Except false
             :- 'Project ['id, 'name]                                     :- 'Project ['id, 'name]
             :  +- 'Filter (1 = 1)                                        :  +- 'Filter (1 = 1)
!            :     +- 'UnresolvedRelation [test], [], false               :     +- 'SubqueryAlias spark_catalog.default.test
!            +- 'Project ['id, 'name]                                     :        +- 'UnresolvedCatalogRelation `default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [], false
!               +- 'Filter 'id IN (1,2)                                   +- 'Project ['id, 'name]
!                  +- 'UnresolvedRelation [test], [], false                  +- 'Filter 'id IN (1,2)
!                                                                               +- 'SubqueryAlias spark_catalog.default.test
!                                                                                  +- 'UnresolvedCatalogRelation `default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [], false

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.execution.datasources.FindDataSourceTable ===
 'Sort ['t ASC NULLS FIRST], true                                                                                                     'Sort ['t ASC NULLS FIRST], true
 +- 'Project ['id, 'name, yes AS t#34]                                                                                                +- 'Project ['id, 'name, yes AS t#34]
    +- 'Filter ('name = tom)                                                                                                             +- 'Filter ('name = tom)
       +- 'SubqueryAlias __auto_generated_subquery_name                                                                                     +- 'SubqueryAlias __auto_generated_subquery_name
          +- 'Except false                                                                                                                     +- 'Except false
             :- 'Project ['id, 'name]                                                                                                             :- 'Project ['id, 'name]
!            :  +- 'Filter (1 = 1)                                                                                                                :  +- Filter (1 = 1)
!            :     +- 'SubqueryAlias spark_catalog.default.test                                                                                   :     +- SubqueryAlias spark_catalog.default.test
!            :        +- 'UnresolvedCatalogRelation `default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [], false               :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
             +- 'Project ['id, 'name]                                                                                                             +- 'Project ['id, 'name]
                +- 'Filter 'id IN (1,2)                                                                                                              +- 'Filter 'id IN (1,2)
!                  +- 'SubqueryAlias spark_catalog.default.test                                                                                         +- SubqueryAlias spark_catalog.default.test
!                     +- 'UnresolvedCatalogRelation `default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [], false                        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveReferences ===
!'Sort ['t ASC NULLS FIRST], true                                                                                                                                            Sort [t#34 ASC NULLS FIRST], true
!+- 'Project ['id, 'name, yes AS t#34]                                                                                                                                       +- Project [id#35, name#36, yes AS t#34]
!   +- 'Filter ('name = tom)                                                                                                                                                    +- Filter (name#36 = tom)
!      +- 'SubqueryAlias __auto_generated_subquery_name                                                                                                                            +- SubqueryAlias __auto_generated_subquery_name
!         +- 'Except false                                                                                                                                                            +- Except false
!            :- 'Project ['id, 'name]                                                                                                                                                    :- Project [id#35, name#36]
             :  +- Filter (1 = 1)                                                                                                                                                        :  +- Filter (1 = 1)
             :     +- SubqueryAlias spark_catalog.default.test                                                                                                                           :     +- SubqueryAlias spark_catalog.default.test
             :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]               :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!            +- 'Project ['id, 'name]                                                                                                                                                    +- Project [id#38, name#39]
!               +- 'Filter 'id IN (1,2)                                                                                                                                                     +- Filter id#38 IN (1,2)
                   +- SubqueryAlias spark_catalog.default.test                                                                                                                                 +- SubqueryAlias spark_catalog.default.test
                      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]                        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Result of Batch Resolution ===
!'Sort ['t ASC NULLS FIRST], true                             Sort [t#34 ASC NULLS FIRST], true
!+- 'Project ['id, 'name, yes AS t#34]                        +- Project [id#35, name#36, yes AS t#34]
!   +- 'Filter ('name = tom)                                     +- Filter (name#36 = tom)
!      +- 'SubqueryAlias __auto_generated_subquery_name             +- SubqueryAlias __auto_generated_subquery_name
!         +- 'Except false                                             +- Except false
!            :- 'Project ['id, 'name]                                     :- Project [id#35, name#36]
!            :  +- 'Filter (1 = 1)                                        :  +- Filter (1 = 1)
!            :     +- 'UnresolvedRelation [test], [], false               :     +- SubqueryAlias spark_catalog.default.test
!            +- 'Project ['id, 'name]                                     :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!               +- 'Filter 'id IN (1,2)                                   +- Project [id#38, name#39]
!                  +- 'UnresolvedRelation [test], [], false                  +- Filter id#38 IN (1,2)
!                                                                               +- SubqueryAlias spark_catalog.default.test
!                                                                                  +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger: Batch Remove TempResolvedColumn has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Apply Char Padding has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Post-Hoc Resolution has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Remove Unresolved Hints has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Nondeterministic has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch UDF has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch UpdateNullability has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Subquery has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.analysis.CleanupAliases ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                           Sort [t#34 ASC NULLS FIRST], true
 +- Project [id#35, name#36, yes AS t#34]                                                                                                                                    +- Project [id#35, name#36, yes AS t#34]
    +- Filter (name#36 = tom)                                                                                                                                                   +- Filter (name#36 = tom)
       +- SubqueryAlias __auto_generated_subquery_name                                                                                                                             +- SubqueryAlias __auto_generated_subquery_name
          +- Except false                                                                                                                                                             +- Except false
             :- Project [id#35, name#36]                                                                                                                                                 :- Project [id#35, name#36]
             :  +- Filter (1 = 1)                                                                                                                                                        :  +- Filter (1 = 1)
             :     +- SubqueryAlias spark_catalog.default.test                                                                                                                           :     +- SubqueryAlias spark_catalog.default.test
             :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]               :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
             +- Project [id#38, name#39]                                                                                                                                                 +- Project [id#38, name#39]
                +- Filter id#38 IN (1,2)                                                                                                                                                    +- Filter id#38 IN (1,2)
                   +- SubqueryAlias spark_catalog.default.test                                                                                                                                 +- SubqueryAlias spark_catalog.default.test
                      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]                        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Result of Batch Cleanup ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                           Sort [t#34 ASC NULLS FIRST], true
 +- Project [id#35, name#36, yes AS t#34]                                                                                                                                    +- Project [id#35, name#36, yes AS t#34]
    +- Filter (name#36 = tom)                                                                                                                                                   +- Filter (name#36 = tom)
       +- SubqueryAlias __auto_generated_subquery_name                                                                                                                             +- SubqueryAlias __auto_generated_subquery_name
          +- Except false                                                                                                                                                             +- Except false
             :- Project [id#35, name#36]                                                                                                                                                 :- Project [id#35, name#36]
             :  +- Filter (1 = 1)                                                                                                                                                        :  +- Filter (1 = 1)
             :     +- SubqueryAlias spark_catalog.default.test                                                                                                                           :     +- SubqueryAlias spark_catalog.default.test
             :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]               :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
             +- Project [id#38, name#39]                                                                                                                                                 +- Project [id#38, name#39]
                +- Filter id#38 IN (1,2)                                                                                                                                                    +- Filter id#38 IN (1,2)
                   +- SubqueryAlias spark_catalog.default.test                                                                                                                                 +- SubqueryAlias spark_catalog.default.test
                      +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]                        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger: Batch HandleAnalysisOnlyCommand has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger:
=== Metrics of Executed Rules ===
Total number of runs: 187
Total time: 0.078240584 seconds
Total number of effective runs: 4
Total time of effective runs: 0.064291595 seconds

21/09/06 00:37:54 WARN PlanChangeLogger: Batch Eliminate Distinct has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                           Sort [t#34 ASC NULLS FIRST], true
 +- Project [id#35, name#36, yes AS t#34]                                                                                                                                    +- Project [id#35, name#36, yes AS t#34]
    +- Filter (name#36 = tom)                                                                                                                                                   +- Filter (name#36 = tom)
!      +- SubqueryAlias __auto_generated_subquery_name                                                                                                                             +- Except false
!         +- Except false                                                                                                                                                             :- Project [id#35, name#36]
!            :- Project [id#35, name#36]                                                                                                                                              :  +- Filter (1 = 1)
!            :  +- Filter (1 = 1)                                                                                                                                                     :     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!            :     +- SubqueryAlias spark_catalog.default.test                                                                                                                        +- Project [id#38, name#39]
!            :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]               +- Filter id#38 IN (1,2)
!            +- Project [id#38, name#39]                                                                                                                                                    +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]
!               +- Filter id#38 IN (1,2)
!                  +- SubqueryAlias spark_catalog.default.test
!                     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Result of Batch Finish Analysis ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                           Sort [t#34 ASC NULLS FIRST], true
 +- Project [id#35, name#36, yes AS t#34]                                                                                                                                    +- Project [id#35, name#36, yes AS t#34]
    +- Filter (name#36 = tom)                                                                                                                                                   +- Filter (name#36 = tom)
!      +- SubqueryAlias __auto_generated_subquery_name                                                                                                                             +- Except false
!         +- Except false                                                                                                                                                             :- Project [id#35, name#36]
!            :- Project [id#35, name#36]                                                                                                                                              :  +- Filter (1 = 1)
!            :  +- Filter (1 = 1)                                                                                                                                                     :     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!            :     +- SubqueryAlias spark_catalog.default.test                                                                                                                        +- Project [id#38, name#39]
!            :        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]               +- Filter id#38 IN (1,2)
!            +- Project [id#38, name#39]                                                                                                                                                    +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]
!               +- Filter id#38 IN (1,2)
!                  +- SubqueryAlias spark_catalog.default.test
!                     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger: Batch Union has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch OptimizeLimitZero has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch LocalRelation early has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Pullup Correlated Expressions has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger: Batch Subquery has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithFilter ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                     Sort [t#34 ASC NULLS FIRST], true
 +- Project [id#35, name#36, yes AS t#34]                                                                                                                              +- Project [id#35, name#36, yes AS t#34]
    +- Filter (name#36 = tom)                                                                                                                                             +- Filter (name#36 = tom)
!      +- Except false                                                                                                                                                       +- Distinct
!         :- Project [id#35, name#36]                                                                                                                                           +- Filter NOT coalesce(id#35 IN (1,2), false)
!         :  +- Filter (1 = 1)                                                                                                                                                     +- Project [id#35, name#36]
!         :     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]                  +- Filter (1 = 1)
!         +- Project [id#38, name#39]                                                                                                                                                    +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!            +- Filter id#38 IN (1,2)
!               +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                        Sort [t#34 ASC NULLS FIRST], true
 +- Project [id#35, name#36, yes AS t#34]                                                                                                                                 +- Project [id#35, name#36, yes AS t#34]
    +- Filter (name#36 = tom)                                                                                                                                                +- Filter (name#36 = tom)
!      +- Distinct                                                                                                                                                              +- Aggregate [id#35, name#36], [id#35, name#36]
          +- Filter NOT coalesce(id#35 IN (1,2), false)                                                                                                                            +- Filter NOT coalesce(id#35 IN (1,2), false)
             +- Project [id#35, name#36]                                                                                                                                              +- Project [id#35, name#36]
                +- Filter (1 = 1)                                                                                                                                                        +- Filter (1 = 1)
                   +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]                     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Result of Batch Replace Operators ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                     Sort [t#34 ASC NULLS FIRST], true
 +- Project [id#35, name#36, yes AS t#34]                                                                                                                              +- Project [id#35, name#36, yes AS t#34]
    +- Filter (name#36 = tom)                                                                                                                                             +- Filter (name#36 = tom)
!      +- Except false                                                                                                                                                       +- Aggregate [id#35, name#36], [id#35, name#36]
!         :- Project [id#35, name#36]                                                                                                                                           +- Filter NOT coalesce(id#35 IN (1,2), false)
!         :  +- Filter (1 = 1)                                                                                                                                                     +- Project [id#35, name#36]
!         :     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]                  +- Filter (1 = 1)
!         +- Project [id#38, name#39]                                                                                                                                                    +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!            +- Filter id#38 IN (1,2)
!               +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#38, name#39, tel#40], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger: Batch Aggregate has no effect.
21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                        Sort [t#34 ASC NULLS FIRST], true
 +- Project [id#35, name#36, yes AS t#34]                                                                                                                                 +- Project [id#35, name#36, yes AS t#34]
!   +- Filter (name#36 = tom)                                                                                                                                                +- Aggregate [id#35, name#36], [id#35, name#36]
!      +- Aggregate [id#35, name#36], [id#35, name#36]                                                                                                                          +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))
!         +- Filter NOT coalesce(id#35 IN (1,2), false)                                                                                                                            +- Project [id#35, name#36]
!            +- Project [id#35, name#36]                                                                                                                                              +- Filter (1 = 1)
!               +- Filter (1 = 1)                                                                                                                                                        +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!                  +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Sort [t#34 ASC NULLS FIRST], true                                                                                                                                     Sort [t#34 ASC NULLS FIRST], true
!+- Project [id#35, name#36, yes AS t#34]                                                                                                                              +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]
!   +- Aggregate [id#35, name#36], [id#35, name#36]                                                                                                                       +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))
!      +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))                                                                                                   +- Project [id#35, name#36]
!         +- Project [id#35, name#36]                                                                                                                                           +- Filter (1 = 1)
!            +- Filter (1 = 1)                                                                                                                                                     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!               +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===
!Sort [t#34 ASC NULLS FIRST], true                                                                                                                                  Sort [yes ASC NULLS FIRST], true
 +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]                                                                                                       +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]
    +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))                                                                                                +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))
       +- Project [id#35, name#36]                                                                                                                                        +- Project [id#35, name#36]
          +- Filter (1 = 1)                                                                                                                                                  +- Filter (1 = 1)
             +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]               +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Sort [yes ASC NULLS FIRST], true                                                                                                                                   Sort [yes ASC NULLS FIRST], true
 +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]                                                                                                       +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]
    +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))                                                                                                +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))
       +- Project [id#35, name#36]                                                                                                                                        +- Project [id#35, name#36]
!         +- Filter (1 = 1)                                                                                                                                                  +- Filter true
             +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]               +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PruneFilters ===
 Sort [yes ASC NULLS FIRST], true                                                                                                                                   Sort [yes ASC NULLS FIRST], true
 +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]                                                                                                       +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]
    +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))                                                                                                +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))
       +- Project [id#35, name#36]                                                                                                                                        +- Project [id#35, name#36]
!         +- Filter true                                                                                                                                                     +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
!            +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]

21/09/06 00:37:54 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Sort [yes ASC NULLS FIRST], true                                                                                                                                Sort [yes ASC NULLS FIRST], true
 +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]                                                                                                    +- Aggregate [id#35, name#36], [id#35, name#36, yes AS t#34]
!   +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))                                                                                             +- Project [id#35, name#36]
!      +- Project [id#35, name#36]                                                                                                                                     +- Filter (NOT coalesce(id#35 IN (1,2), false) AND (name#36 = tom))
          +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]            +- HiveTableRelation [`default`.`test`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [id#35, name#36, tel#37], Partition Cols: []]
```

# 作业3
见homework3/src/main/scala/MyPushDown
