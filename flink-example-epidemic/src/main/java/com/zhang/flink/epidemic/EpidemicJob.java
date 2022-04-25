package com.zhang.flink.epidemic;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple19;


public class EpidemicJob {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        CsvReader csvReader = environment.readCsvFile("D:\\java_project\\Flink\\flink-example-epidemic\\src\\main\\resources\\DXYArea.csv").ignoreFirstLine();
        DataSource<Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>> dataSource = csvReader.types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
        dataSource
                .filter(value -> value.f2.equals("中国"))
                .filter(value -> value.f14.matches("[0-9][0-9][0-9][0-9][0-9][0-9]"))
                .map(EpidemicModel::new)
                .filter(value -> value.getCityName().equals("武汉"))
                .sortPartition(new KeySelector<EpidemicModel, Long>() {
                    @Override
                    public Long getKey(EpidemicModel value) throws Exception {
                        return value.getTime();
                    }
                }, Order.ASCENDING)
                .output(new EpidemicsFormat());

        environment.execute();
    }
}
