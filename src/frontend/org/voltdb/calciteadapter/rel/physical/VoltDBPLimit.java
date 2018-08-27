/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter.rel.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;

public class VoltDBPLimit extends SingleRel implements VoltDBPRel {

    // @TODO limit / offset as expressions or parameters
    private RexNode m_offset;
    private RexNode m_limit;

    private final int m_splitCount;

    public VoltDBPLimit(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexNode offset,
            RexNode limit,
            int splitCount) {
            super(cluster, traitSet, input);
            assert traitSet.contains(VoltDBPRel.VOLTDB_PHYSICAL);
            m_offset = offset;
            m_limit = limit;
            m_splitCount = splitCount;
        }

    public VoltDBPLimit copy(RelTraitSet traitSet, RelNode input,
            RexNode offset, RexNode limit, int splitCount) {
        return new VoltDBPLimit(
                getCluster(),
                traitSet,
                input,
                offset,
                limit,
                splitCount);
    }

    @Override
    public VoltDBPLimit copy(RelTraitSet traitSet,
            List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), m_offset, m_limit, m_splitCount);
    }

    @Override
    public AbstractPlanNode toPlanNode() {

        LimitPlanNode lpn = new LimitPlanNode();
        lpn = new LimitPlanNode();
        if (m_limit != null) {
            lpn.setLimit(RexLiteral.intValue(m_limit));
        }
        if (m_offset != null) {
            lpn.setOffset(RexLiteral.intValue(m_offset));
        }

        if (this.getInput() != null) {
            // Limit is not inlined
            AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
            lpn.addAndLinkChild(child);
        }

        return lpn;

    }

    public RexNode getOffset() {
        return m_offset;
    }

    public RexNode getLimit() {
        return m_limit;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("split", m_splitCount);
        if (m_limit != null) {
            pw.item("limit", m_limit);
        }
        if (m_offset != null) {
            pw.item("offset", m_offset);
        }
        return pw;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        digest += "_split_" + m_splitCount;
        return digest;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double limit = 0f;
        if (m_limit != null) {
            limit = RexLiteral.intValue(m_limit);
        }
        if (m_offset != null) {
            limit += RexLiteral.intValue(m_offset);
        }
        double defaultLimit = super.estimateRowCount(mq);
        if (limit == 0f || defaultLimit < limit) {
            limit = defaultLimit;
        }
        return limit;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double rowCount = estimateRowCount(mq);
        // Hack. Discourage Calcite from picking a plan with a Limit that have a RelDistributions.ANY
        // distribution trait. This would make a "correct"
        // VoltDBPLimit (Single) / DistributedExchange / VoltDBPLimit (Hash) plan
        // less expensive than an "incorrect" VoltDBPLimit (Any) / DistributedExchange one.
        if (RelDistributions.ANY.getType().equals(getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE).getType())) {
            rowCount *= 10000;
        }

        RelOptCost defaultCost = super.computeSelfCost(planner, mq);
        return planner.getCostFactory().makeCost(rowCount, defaultCost.getCpu(), defaultCost.getIo());
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

}