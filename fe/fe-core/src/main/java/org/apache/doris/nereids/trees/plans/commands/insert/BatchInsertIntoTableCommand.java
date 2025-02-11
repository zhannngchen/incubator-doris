// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.InlineTable;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.NoForward;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * insert into values with in txn model.
 */
public class BatchInsertIntoTableCommand extends Command implements NoForward, Explainable {

    public static final Logger LOG = LogManager.getLogger(BatchInsertIntoTableCommand.class);

    private LogicalPlan originLogicalQuery;
    private Optional<LogicalPlan> logicalQuery;

    public BatchInsertIntoTableCommand(LogicalPlan logicalQuery) {
        super(PlanType.BATCH_INSERT_INTO_TABLE_COMMAND);
        this.originLogicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.logicalQuery = Optional.empty();
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery.orElse(originLogicalQuery);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) throws Exception {
        Optional<CascadesContext> analyzeContext = Optional.of(
                CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY)
        );
        return InsertUtils.getPlanForExplain(ctx, analyzeContext, getLogicalQuery());
    }

    @Override
    public Optional<NereidsPlanner> getExplainPlanner(LogicalPlan logicalPlan, StatementContext ctx) throws Exception {
        ConnectContext connectContext = ctx.getConnectContext();
        TableIf targetTableIf = InsertUtils.getTargetTable(originLogicalQuery, connectContext);
        boolean supportFastInsertIntoValues
                = InsertUtils.supportFastInsertIntoValues(logicalPlan, targetTableIf, connectContext);
        return Optional.of(new FastInsertIntoValuesPlanner(ctx, supportFastInsertIntoValues));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitBatchInsertIntoTableCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        UnboundTableSink<? extends Plan> unboundTableSink = (UnboundTableSink<? extends Plan>) originLogicalQuery;
        Plan query = unboundTableSink.child();
        if (!(query instanceof InlineTable)) {
            throw new AnalysisException("Insert into ** select is not supported in a transaction");
        }

        PhysicalOlapTableSink<?> sink;
        TableIf targetTableIf = InsertUtils.getTargetTable(originLogicalQuery, ctx);
        targetTableIf.readLock();
        try {
            StatementContext statementContext = ctx.getStatementContext();
            Optional<CascadesContext> analyzeContext = Optional.of(
                    CascadesContext.initContext(statementContext, originLogicalQuery, PhysicalProperties.ANY)
            );

            this.logicalQuery = Optional.of((LogicalPlan) InsertUtils.normalizePlan(
                originLogicalQuery, targetTableIf, analyzeContext, Optional.empty()
            ));

            LogicalPlan logicalQuery = this.logicalQuery.get();
            LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, statementContext);

            boolean supportFastInsertIntoValues
                    = InsertUtils.supportFastInsertIntoValues(logicalQuery, targetTableIf, ctx);
            FastInsertIntoValuesPlanner planner = new FastInsertIntoValuesPlanner(
                    statementContext, supportFastInsertIntoValues, true);
            planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
            executor.checkBlockRules();
            if (ctx.getConnectType() == ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
                ctx.getMysqlChannel().reset();
            }

            Optional<TreeNode<?>> plan = planner.getPhysicalPlan()
                    .<TreeNode<?>>collect(PhysicalOlapTableSink.class::isInstance).stream().findAny();
            Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
            sink = ((PhysicalOlapTableSink<?>) plan.get());
            Table targetTable = sink.getTargetTable();
            if (ctx.getTxnEntry().isFirstTxnInsert()) {
                ctx.getTxnEntry().setTxnSchemaVersion(((OlapTable) targetTable).getBaseSchemaVersion());
                ctx.getTxnEntry().setFirstTxnInsert(false);
            } else {
                if (((OlapTable) targetTable).getBaseSchemaVersion() != ctx.getTxnEntry().getTxnSchemaVersion()) {
                    throw new AnalysisException("There are schema changes in one transaction, "
                            + "you can commit this transaction with formal data or rollback "
                            + "this whole transaction.");
                }
            }
            // should set columns of sink since we maybe generate some invisible columns
            List<Column> fullSchema = sink.getTargetTable().getFullSchema();
            List<Column> targetSchema = Lists.newArrayList();
            if (sink.isPartialUpdate()) {
                List<String> partialUpdateColumns = sink.getCols().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
                for (Column column : fullSchema) {
                    if (partialUpdateColumns.contains(column.getName())) {
                        targetSchema.add(column);
                    }
                }
            } else {
                targetSchema = removeSkipBitmapCol(fullSchema);
            }
            // check auth
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), targetTable.getDatabase().getCatalog().getName(),
                            targetTable.getQualifiedDbName(), targetTable.getName(),
                            PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        targetTable.getQualifiedDbName() + ": " + targetTable.getName());
            }

            Optional<PhysicalUnion> union = planner.getPhysicalPlan()
                    .<PhysicalUnion>collect(PhysicalUnion.class::isInstance).stream().findAny();
            if (union.isPresent()) {
                InsertUtils.executeBatchInsertTransaction(ctx, targetTable.getQualifiedDbName(), targetTable.getName(),
                        targetSchema, reorderUnionData(removeSkipBitmapExpr(sink.getOutputExprs()),
                                union.get().getOutputs(), union.get().getConstantExprsList()));
                return;
            }
            Optional<PhysicalOneRowRelation> oneRowRelation = planner.getPhysicalPlan()
                    .<PhysicalOneRowRelation>collect(PhysicalOneRowRelation.class::isInstance).stream().findAny();
            if (oneRowRelation.isPresent()) {
                InsertUtils.executeBatchInsertTransaction(ctx, targetTable.getQualifiedDbName(),
                        targetTable.getName(), targetSchema,
                        ImmutableList.of(
                                reorderOneRowData(removeSkipBitmapExpr(sink.getOutputExprs()),
                                        oneRowRelation.get().getProjects())));
                return;
            }
            // TODO: update error msg
            throw new AnalysisException("could not run this sql");
        } finally {
            targetTableIf.readUnlock();
        }
    }

    private List<NamedExpression> removeSkipBitmapExpr(List<NamedExpression> sinkExprs) {
        return sinkExprs.stream().filter(expr -> !Column.SKIP_BITMAP_COL.equals(expr.getName()))
                .collect(Collectors.toList());
    }

    private List<Column> removeSkipBitmapCol(List<Column> columns) {
        return columns.stream().filter(col -> !Column.SKIP_BITMAP_COL.equals(col.getName()))
                .collect(Collectors.toList());
    }

    // If table schema is c1, c2, c3, we do insert into table (c3, c2, c1) values(v3, v2, v1).
    // The oneRowExprts are [v3#c1, v2#c2, v1#c3], which is wrong sequence. The sinkExprs are
    // [v1#c3, v2#c2, v3#c1]. However, sinkExprs are SlotRefrence rather than Alias. We need to
    // extract right sequence alias from oneRowExprs.
    private List<NamedExpression> reorderOneRowData(List<NamedExpression> sinkExprs,
            List<NamedExpression> oneRowExprs) {
        List<NamedExpression> sequenceData = new ArrayList<>();
        for (NamedExpression expr : sinkExprs) {
            for (NamedExpression project : oneRowExprs) {
                if (expr.getExprId().equals(project.getExprId())) {
                    sequenceData.add(project);
                    break;
                }
            }
        }
        return sequenceData;
    }

    private List<List<NamedExpression>> reorderUnionData(List<NamedExpression> sinkExprs,
            List<NamedExpression> unionOutputs, List<List<NamedExpression>> unionExprs) {
        Map<ExprId, Integer> indexMap = new HashMap<>();
        for (int i = 0; i < unionOutputs.size(); i++) {
            indexMap.put(unionOutputs.get(i).getExprId(), i);
        }

        List<List<NamedExpression>> reorderedExprs = new ArrayList<>();
        for (List<NamedExpression> exprList : unionExprs) {
            List<NamedExpression> reorderedList = new ArrayList<>();
            for (NamedExpression expr : sinkExprs) {
                int index = indexMap.get(expr.getExprId());
                reorderedList.add(exprList.get(index));
            }
            reorderedExprs.add(reorderedList);
        }
        return reorderedExprs;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.INSERT;
    }
}
