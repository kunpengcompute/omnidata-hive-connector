package org.apache.hadoop.hive.ql.omnidata;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;

import com.huawei.boostkit.omnidata.model.Column;
import com.huawei.boostkit.omnidata.model.Predicate;
import com.huawei.boostkit.omnidata.type.BooleanDecodeType;
import com.huawei.boostkit.omnidata.type.ByteDecodeType;
import com.huawei.boostkit.omnidata.type.DateDecodeType;
import com.huawei.boostkit.omnidata.type.DecodeType;
import com.huawei.boostkit.omnidata.type.DoubleDecodeType;
import com.huawei.boostkit.omnidata.type.FloatDecodeType;
import com.huawei.boostkit.omnidata.type.IntDecodeType;
import com.huawei.boostkit.omnidata.type.LongDecodeType;
import com.huawei.boostkit.omnidata.type.LongToByteDecodeType;
import com.huawei.boostkit.omnidata.type.LongToFloatDecodeType;
import com.huawei.boostkit.omnidata.type.LongToIntDecodeType;
import com.huawei.boostkit.omnidata.type.LongToShortDecodeType;
import com.huawei.boostkit.omnidata.type.ShortDecodeType;
import com.huawei.boostkit.omnidata.type.VarcharDecodeType;

import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OmniData tool
 *
 * @since 2021-11-16
 */
public class OmniDataUtils {

    /**
     * Converts Hive data type to OmniData Type
     *
     * @param dataType Hive data type
     * @return OmniData Type
     */
    public static Type transOmniDataType(String dataType) {
        String lType = dataType.toLowerCase(Locale.ENGLISH);
        // Keep the English letters and remove the others. like: char(11) -> char
        if (lType.contains("char")) {
            lType = dataType.replaceAll("[^a-z<>]", "");
        }
        switch (lType) {
            case "bigint":
            case "long":
                return BIGINT;
            case "boolean":
                return BOOLEAN;
            case "byte":
            case "tinyint":
                return TINYINT;
            case "char":
            case "string":
            case "varchar":
                return VARCHAR;
            case "double":
                return DOUBLE;
            case "date":
                return DATE;
            case "float":
                return REAL;
            case "int":
            case "integer":
                return INTEGER;
            case "short":
            case "smallint":
                return SMALLINT;
            case "array<string>":
                return new ArrayType<>(VARCHAR);
            default:
                throw new UnsupportedOperationException("OmniData Hive unsupported this type:" + lType);
        }
    }

    /**
     * Converts Hive UDF typeInfo to OmniData Type
     *
     * @param typeInfo Hive UDF typeInfo
     * @return OmniData Type
     */
    public static Type transOmniDataUdfType(TypeInfo typeInfo) {
        if (typeInfo instanceof CharTypeInfo) {
            return CharType.createCharType(((CharTypeInfo) typeInfo).getLength());
        } else if (typeInfo instanceof VarcharTypeInfo) {
            return VarcharType.createVarcharType(((VarcharTypeInfo) typeInfo).getLength());
        } else {
            return transOmniDataType(typeInfo.getTypeName());
        }
    }

    public static Type transAggType(Type dataType) {
        if (BIGINT.equals(dataType) || INTEGER.equals(dataType) || SMALLINT.equals(dataType) || TINYINT.equals(
            dataType)) {
            return BIGINT;
        } else if (DOUBLE.equals(dataType) || REAL.equals(dataType)) {
            return DOUBLE;
        } else {
            return dataType;
        }
    }

    /**
     * Converts Hive Agg return type to OmniData DecodeType
     *
     * @param dataType Hive Agg return type
     * @return OmniData DecodeType
     */
    public static DecodeType transOmniDataAggDecodeType(String dataType) {
        String lType = dataType.toLowerCase(Locale.ENGLISH);
        // Keep the English letters and remove the others. like: char(11) -> char
        if (lType.contains("char")) {
            lType = dataType.replaceAll("[^a-z<>]", "");
        }
        switch (lType) {
            case "bigint":
                return new LongDecodeType();
            case "boolean":
                return new BooleanDecodeType();
            case "byte":
            case "tinyint":
                return new LongToByteDecodeType();
            case "char":
            case "string":
            case "varchar":
                return new VarcharDecodeType();
            case "date":
            case "int":
            case "integer":
                return new LongToIntDecodeType();
            case "double":
                return new DoubleDecodeType();
            case "float":
            case "real":
                return new LongToFloatDecodeType();
            case "smallint":
                return new LongToShortDecodeType();
            default:
                throw new UnsupportedOperationException("OmniData Hive unsupported this type:" + lType);
        }
    }

    /**
     * Converts Hive return type to OmniData DecodeType
     *
     * @param dataType Hive return type
     * @return OmniData DecodeType
     */
    public static DecodeType transOmniDataDecodeType(String dataType) {
        String lType = dataType.toLowerCase(Locale.ENGLISH);
        // Keep the English letters and remove the others. like: char(11) -> char
        if (lType.contains("char")) {
            lType = dataType.replaceAll("[^a-z<>]", "");
        }
        switch (lType) {
            case "bigint":
                return new LongDecodeType();
            case "boolean":
                return new BooleanDecodeType();
            case "byte":
            case "tinyint":
                return new ByteDecodeType();
            case "char":
            case "string":
            case "varchar":
                return new VarcharDecodeType();
            case "date":
                return new DateDecodeType();
            case "double":
                return new DoubleDecodeType();
            case "float":
            case "real":
                return new FloatDecodeType();
            case "int":
            case "integer":
                return new IntDecodeType();
            case "smallint":
                return new ShortDecodeType();
            default:
                throw new UnsupportedOperationException("OmniData Hive unsupported this type:" + lType);
        }
    }

    /**
     * Converts OmniData Type to ConstantExpression
     *
     * @param value constant value
     * @param type OmniData Type
     * @return ConstantExpression
     */
    public static ConstantExpression transOmniDataConstantExpr(String value, Type type) {
        // check 'null' value
        if (value.equals("null") && type != VARCHAR) {
            return new ConstantExpression(null, type);
        }
        switch (type.toString().toLowerCase(Locale.ENGLISH)) {
            case "bigint":
            case "integer":
            case "tinyint":
            case "smallint":
                return new ConstantExpression(Long.parseLong(value), type);
            case "boolean":
                return new ConstantExpression(Boolean.parseBoolean(value), type);
            case "date":
                if (value.contains("-")) {
                    String[] dateStrArray = value.split("-");
                    long daToMillSecs = 24 * 3600 * 1000;
                    int year = Integer.parseInt(dateStrArray[0]) - 1900;
                    int month = Integer.parseInt(dateStrArray[1]) - 1;
                    int day = Integer.parseInt(dateStrArray[2]);
                    java.sql.Date date = new java.sql.Date(year, month, day);
                    return new ConstantExpression((date.getTime() - date.getTimezoneOffset() * 60000L) / daToMillSecs,
                        type);
                } else {
                    return new ConstantExpression(Long.parseLong(value), type);
                }
            case "double":
                return new ConstantExpression(Double.parseDouble(value), type);
            case "real":
                return new ConstantExpression((long) floatToIntBits(Float.parseFloat(value)), type);
            case "varchar":
                return new ConstantExpression(utf8Slice(value), type);
            default:
                throw new UnsupportedOperationException(
                    "OmniData Hive unsupported this type:" + type.toString().toLowerCase(Locale.ENGLISH));
        }
    }

    /**
     * Converts Hive operator name to OmniData operator
     *
     * @param operator Hive operator name
     * @return OmniData operator
     */
    public static String transOmniDataOperator(String operator) {
        switch (operator) {
            case "LESS_THAN":
                return "less_than";
            case "LESS_THAN_EQUALS":
                return "less_than_or_equal";
            case "EQUALS":
                return "equal";
            default:
                return operator;
        }
    }

    public static NdpPredicateInfo addPartitionValues(NdpPredicateInfo oldNdpPredicate, String path,
        String defaultPartitionValue) {
        Predicate oldPredicate = oldNdpPredicate.getPredicate();
        List<Column> newColumns = new ArrayList<>();
        for (Column column : oldPredicate.getColumns()) {
            if (column.isPartitionKey()) {
                String partitionValue = getPartitionValue(path, column.getName(), defaultPartitionValue);
                newColumns.add(
                    new Column(column.getFieldId(), column.getName(), column.getType(), true, partitionValue));
            } else {
                newColumns.add(column);
            }
        }
        Predicate newPredicate = new Predicate(oldPredicate.getTypes(), newColumns, oldPredicate.getFilter(),
            oldPredicate.getProjections(), oldPredicate.getDomains(), oldPredicate.getBloomFilters(),
            oldPredicate.getAggregations(), oldPredicate.getLimit());
        return new NdpPredicateInfo(oldNdpPredicate.getIsPushDown(), oldNdpPredicate.getIsPushDownAgg(),
            oldNdpPredicate.getIsPushDownFilter(), oldNdpPredicate.getHasPartitionColumn(), newPredicate,
            oldNdpPredicate.getOutputColumns(), oldNdpPredicate.getDecodeTypes(),
            oldNdpPredicate.getDecodeTypesWithAgg());
    }

    private static String getPartitionValue(String filePath, String columnName, String defaultPartitionValue) {
        String[] filePathStrArray = filePath.split("\\/");
        String partitionValue = "";
        Pattern pn = Pattern.compile(columnName + "\\=");
        for (String strColumn : filePathStrArray) {
            Matcher matcher = pn.matcher(strColumn);
            if (matcher.find()) {
                partitionValue = strColumn.split("\\=")[1];
                if (defaultPartitionValue.equals(partitionValue)) {
                    partitionValue = null;
                }
                break;
            }
        }
        return partitionValue;
    }
}

