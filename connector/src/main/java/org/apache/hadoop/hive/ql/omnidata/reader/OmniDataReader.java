package org.apache.hadoop.hive.ql.omnidata.reader;

import com.huawei.boostkit.omnidata.model.datasource.DataSource;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * OmniDataReader for agg optimization
 *
 * @since 2022-03-07
 */
public class OmniDataReader implements Callable {


    private OmniDataProperty omniDataProperty;
    private DataSource dataSource;
    private NdpPredicateInfo ndpPredicateInfo;

    public OmniDataReader(DataSource dataSource, OmniDataProperty omniDataProperty, NdpPredicateInfo ndpPredicateInfo) {
        this.dataSource = dataSource;
        this.omniDataProperty = omniDataProperty;
        this.ndpPredicateInfo = ndpPredicateInfo;
    }


    @Override
    public List<VectorizedRowBatch> call() throws UnknownHostException {
        OmniDataAdapter omniDataAdapter = new OmniDataAdapter(dataSource, omniDataProperty, ndpPredicateInfo);
        Queue<ColumnVector[]> pages = omniDataAdapter.getBatchFromOmniData();
        List<VectorizedRowBatch> vectorizedRowBatch = getVectorizedRowBatch(pages);
        return  vectorizedRowBatch;
    }

    private List<VectorizedRowBatch> getVectorizedRowBatch(Queue<ColumnVector[]> pages){
        List<VectorizedRowBatch> rowBatches = new ArrayList<>();
        if (!pages.isEmpty()) {
            ColumnVector[] columnVectors = pages.poll();
            int channelCount = columnVectors.length;
            int positionCount = columnVectors[0].isNull.length;
            VectorizedRowBatch rowBatch = new VectorizedRowBatch(channelCount, positionCount);
            System.arraycopy(columnVectors, 0, rowBatch.cols, 0, channelCount);
            rowBatches.add(rowBatch);
        }
        return  rowBatches;
    }
}