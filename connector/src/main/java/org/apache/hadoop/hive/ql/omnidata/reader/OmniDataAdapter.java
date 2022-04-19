package org.apache.hadoop.hive.ql.omnidata.reader;

import static org.apache.hadoop.hive.ql.omnidata.OmniDataUtils.addPartitionValues;

import com.huawei.boostkit.omnidata.exception.OmniDataException;
import com.huawei.boostkit.omnidata.exception.OmniErrorCode;
import com.huawei.boostkit.omnidata.model.Predicate;
import com.huawei.boostkit.omnidata.model.TaskSource;
import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsDataSource;
import com.huawei.boostkit.omnidata.reader.impl.DataReaderImpl;
import com.huawei.boostkit.omnidata.type.DecodeType;

import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.decode.PageDeserializer;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * OmniDataAdapter
 */
public class OmniDataAdapter implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OmniDataAdapter.class);

    private static final int TASK_FAILED_TIMES = 4;

    private DataSource dataSource;

    private Queue<ColumnVector[]> batchVectors;

    private NdpPredicateInfo ndpPredicateInfo;

    private OmniDataProperty omniDataProperty;

    private List<String> omniDataHosts;

    OmniDataAdapter(DataSource dataSource, OmniDataProperty omniDataProperty, NdpPredicateInfo ndpPredicateInfo) {
        this.dataSource = dataSource;
        if (dataSource instanceof HdfsDataSource && ndpPredicateInfo.getHasPartitionColumn()) {
            this.ndpPredicateInfo = addPartitionValues(ndpPredicateInfo, ((HdfsDataSource) dataSource).getPath(),
                omniDataProperty.getDefaultPartitionValues());
        } else {
            this.ndpPredicateInfo = ndpPredicateInfo;
        }
        this.omniDataProperty = omniDataProperty;
        omniDataHosts = omniDataProperty.getOmniDataHosts();
    }

    private Queue<ColumnVector[]> getBatchFromOmniData() {
        Predicate predicate = ndpPredicateInfo.getPredicate();
        TaskSource taskSource = new TaskSource(dataSource, predicate, 1048576);
        DecodeType[] columnTypes = new DecodeType[ndpPredicateInfo.getDecodeTypes().size()];
        for (int index = 0; index < columnTypes.length; index++) {
            String codeType = ndpPredicateInfo.getDecodeTypes().get(index);
            if (ndpPredicateInfo.getDecodeTypesWithAgg().get(index)) {
                columnTypes[index] = OmniDataUtils.transOmniDataAggDecodeType(codeType);
            } else {
                columnTypes[index] = OmniDataUtils.transOmniDataDecodeType(codeType);
            }
        }

        PageDeserializer deserializer = new PageDeserializer(columnTypes);

        Queue<ColumnVector[]> pages = new LinkedList<>();
        int failedTimes = 0;
        for (String omniDataHost : omniDataHosts) {
            omniDataProperty.getProperties().put("grpc.client.target", omniDataHost + ":" + omniDataProperty.getPort());
            DataReaderImpl<PageDeserializer> orcDataReader = new DataReaderImpl<>(omniDataProperty.getProperties(),
                taskSource, deserializer);
            try {
                boolean closed = false;
                while (!closed) {
                    List<ColumnVector[]> page = (List<ColumnVector[]>) orcDataReader.getNextPageBlocking();
                    if (page != null) {
                        pages.addAll(page);
                    }
                    if (orcDataReader.isFinished()) {
                        closed = true;
                        break;
                    }
                }
                orcDataReader.close();
                break;
            } catch (OmniDataException omniDataException) {
                LOGGER.warn("OmniData Exception: " + omniDataException.getMessage());
                OmniErrorCode errorCode = omniDataException.getErrorCode();
                switch (errorCode) {
                    case OMNIDATA_INSUFFICIENT_RESOURCES:
                        LOGGER.warn(
                            "OMNIDATA_INSUFFICIENT_RESOURCES: OmniData Server's push down queue is full, begin to find next OmniData-server");
                        break;
                    case OMNIDATA_UNSUPPORTED_OPERATOR:
                        LOGGER.warn("OMNIDATA_UNSUPPORTED_OPERATOR: Exist unsupported operator");
                        break;
                    case OMNIDATA_GENERIC_ERROR:
                        LOGGER.warn(
                            "OMNIDATA_GENERIC_ERROR: Current OmniData Server unavailable, begin to find next OmniData Server");
                        break;
                    case OMNIDATA_NOT_FOUND:
                        LOGGER.warn(
                            "OMNIDATA_NOT_FOUND: Current OmniData Server not found, begin to find next OmniData Server");
                        break;
                    case OMNIDATA_INVALID_ARGUMENT:
                        LOGGER.warn("OMNIDATA_INVALID_ARGUMENT: Exist unsupported operator or datatype");
                        break;
                    case OMNIDATA_IO_ERROR:
                        LOGGER.warn(
                            "OMNIDATA_IO_ERROR: Current OmniData Server io exception, begin to find next OmniData Server");
                        break;
                    default:
                        LOGGER.warn("OmniDataException: OMNIDATA_ERROR.");
                }
                LOGGER.warn("OmniDataAdapter failed node info [hostname :{}]", omniDataHost);
                failedTimes++;
            } catch (Exception e) {
                LOGGER.error("OmniDataAdapter getBatchFromOmnidata() has error:", e);
                failedTimes++;
            }
        }
        int retryTime = Math.min(TASK_FAILED_TIMES, omniDataHosts.size());
        if (failedTimes >= retryTime) {
            LOGGER.warn("No OmniData Server to connect, task has tried {} times.", retryTime);
            throw new TaskExecutionException("No OmniData Server to connect");
        }
        return pages;
    }

    public boolean nextBatchFromOmniData(VectorizedRowBatch batch) {
        if (batchVectors == null) {
            batchVectors = getBatchFromOmniData();
        }
        if (!batchVectors.isEmpty()) {
            ColumnVector[] batchVector = batchVectors.poll();
            // channelCount: column, positionCount: row
            int channelCount = batchVector.length;
            int positionCount = batchVector[0].isNull.length;
            if (ndpPredicateInfo.getIsPushDownAgg()) {
                // agg raw return
                System.arraycopy(batchVector, 0, batch.cols, 0, channelCount);
            } else {
                for (int i = 0; i < channelCount; i++) {
                    int columnId = ndpPredicateInfo.getOutputColumns().get(i);
                    batch.cols[columnId] = batchVector[i];
                }
            }
            batch.size = positionCount;
            return true;
        }
        return false;
    }

}

