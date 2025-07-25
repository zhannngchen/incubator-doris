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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Literal for DecimalV3 Type
 */
public class DecimalV3Literal extends FractionalLiteral {
    private static final Logger logger = Logger.getLogger(Literal.class);

    private final BigDecimal value;

    public DecimalV3Literal(BigDecimal value) {
        super(DecimalV3Type.createDecimalV3Type(value));
        this.value = Objects.requireNonNull(value);
    }

    /**
     * Constructor for DecimalV3Literal
     */
    public DecimalV3Literal(DecimalV3Type dataType, BigDecimal value) {
        super(DecimalV3Type.createDecimalV3TypeLooseCheck(
                dataType.getPrecision() == -1 ? value.precision() : dataType.getPrecision(),
                dataType.getScale() == -1 ? value.scale() : dataType.getScale())
        );

        int precision = dataType.getPrecision() == -1 ? value.precision() : dataType.getPrecision();
        int scale = dataType.getScale() == -1 ? value.scale() : dataType.getScale();
        Objects.requireNonNull(value, "value not be null");
        checkPrecisionAndScale(precision, scale, value);
        BigDecimal adjustedValue = value.scale() < 0 ? value : value.setScale(scale, RoundingMode.HALF_UP);
        logger.info("DecimalV3Literal orig bigDecimal: " + value
                + ", targetType: " + dataType + ", result big decimal: " + adjustedValue);
        this.value = Objects.requireNonNull(adjustedValue);
    }

    @Override
    public BigDecimal getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDecimalV3Literal(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DecimalLiteral(value, getDataType().toCatalogDataType());
    }

    @Override
    public double getDouble() {
        return value.doubleValue();
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return value;
    }

    /**
     * get ceiling of a decimal v3 literal
     * @param newScale scale we want to cast to
     * @return new decimal v3 literal with new scalar
     */
    public DecimalV3Literal roundCeiling(int newScale) {
        if (newScale >= this.getValue().scale()) {
            return this;
        }
        return new DecimalV3Literal(value.setScale(newScale, RoundingMode.CEILING));
    }

    /**
     * get floor of a decimal v3 literal
     * @param newScale scale we want to cast to
     * @return new decimal v3 literal with new scalar
     */
    public DecimalV3Literal roundFloor(int newScale) {
        if (newScale >= this.getValue().scale()) {
            return this;
        }
        return new DecimalV3Literal(value.setScale(newScale, RoundingMode.FLOOR));
    }

    /**
     * get round of a decimal v3 literal
     * @param newScale scale we want to cast to
     * @return new decimal v3 literal with new scalar
     */
    public DecimalV3Literal round(int newScale) {
        if (newScale >= this.getValue().scale()) {
            return this;
        }
        return new DecimalV3Literal(value.setScale(newScale, RoundingMode.HALF_UP));
    }

    /**
     * check precision and scale is enough for value.
     */
    private static void checkPrecisionAndScale(int precision, int scale, BigDecimal value) throws AnalysisException {
        Preconditions.checkNotNull(value);
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return;
        }
        int realPrecision = value.precision();
        int realScale = value.scale();
        boolean valid = true;
        if (precision != -1 && scale != -1) {
            if (precision < realPrecision || scale < realScale || precision - scale < realPrecision - realScale) {
                valid = false;
            }
        } else {
            valid = false;
        }

        if (!valid) {
            throw new CastException(
                    String.format("Invalid precision and scale - expect (%d, %d), but (%d, %d)",
                            precision, scale, realPrecision, realScale));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DecimalV3Literal literal = (DecimalV3Literal) o;
        return Objects.equals(dataType, literal.dataType);
    }

    @Override
    public String computeToSql() {
        return value.toPlainString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
