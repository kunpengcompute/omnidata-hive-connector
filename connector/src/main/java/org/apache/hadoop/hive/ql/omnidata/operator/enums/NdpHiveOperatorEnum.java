package org.apache.hadoop.hive.ql.omnidata.operator.enums;

import org.apache.hadoop.hive.ql.omnidata.physical.NdpPlanChecker;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;

/**
 * Hive operators supported by push down
 *
 * @since 2022-02-24
 */
public enum NdpHiveOperatorEnum {
    // Supported push-down hive operator
    AND("and", GenericUDFOPAnd.class),
    BETWEEN("between", GenericUDFBetween.class),
    EQUAL("=", GenericUDFOPEqual.class),
    GREATER_THAN(">", GenericUDFOPGreaterThan.class),
    GREATER_THAN_OR_EQUAL(">=", GenericUDFOPEqualOrGreaterThan.class),
    IN("in", GenericUDFIn.class),
    LESS_THAN("<", GenericUDFOPLessThan.class),
    LESS_THAN_OR_EQUAL("<=", GenericUDFOPEqualOrLessThan.class),
    LIKE("like", UDFLike.class),
    NOT("not", GenericUDFOPNot.class),
    NOT_EQUAL("!=", GenericUDFOPNotEqual.class),
    NOT_NULL("isnotnull", GenericUDFOPNotNull.class),
    NULL("isnull", GenericUDFOPNull.class),
    OR("or", GenericUDFOPOr.class),
    UNSUPPORTED("unsupported", null);

    private String hiveOpName;

    private Class hiveOpClass;

    NdpHiveOperatorEnum(String hiveOpName, Class hiveOpClass) {
        this.hiveOpName = hiveOpName;
        this.hiveOpClass = hiveOpClass;
    }

    public String getHiveOpName() {
        return hiveOpName;
    }

    public void setHiveOpName(String hiveOpName) {
        this.hiveOpName = hiveOpName;
    }

    public Class getHiveOpClass() {
        return hiveOpClass;
    }

    public void setHiveOpClass(Class hiveOpClass) {
        this.hiveOpClass = hiveOpClass;
    }

    public static NdpHiveOperatorEnum getNdpHiveOperator(ExprNodeGenericFuncDesc funcDesc) {
        NdpHiveOperatorEnum resOperator = NdpHiveOperatorEnum.UNSUPPORTED;
        Class operator = (funcDesc.getGenericUDF() instanceof GenericUDFBridge)
            ? ((GenericUDFBridge) funcDesc.getGenericUDF()).getUdfClass()
            : funcDesc.getGenericUDF().getClass();
        for (NdpHiveOperatorEnum operatorEnum : NdpHiveOperatorEnum.values()) {
            if (operatorEnum.getHiveOpClass() == operator) {
                resOperator = operatorEnum;
                break;
            }
        }
        return resOperator;
    }

    public static boolean checkNotSupportedOperator(NdpHiveOperatorEnum operator) {
        return !(operator.equals(NOT) || operator.equals(AND) || operator.equals(OR) || operator.equals(UNSUPPORTED));
    }
}

