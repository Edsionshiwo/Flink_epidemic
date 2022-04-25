package com.zhang.flink.weather;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple4;

public class WeatherJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        CsvReader reader = environment.readCsvFile("D:\\java_project\\filnk\\flink-example-api\\src\\main\\resources\\loudi_weather.csv").ignoreFirstLine();
        DataSource<Tuple4<String, String, String, String>> dataSource = reader.types(String.class, String.class, String.class, String.class);
//        //最高温
//        dataSource.map(WeatherModel::new)
//                .reduce(((value1, value2) -> value1.getTemperature_day()>=value2.getTemperature_night()?value1:value2))
//                .print();

//        //平均温度
//        long count = dataSource.map(WeatherModel::new).count();
//        MapOperator<Double, Double> map = dataSource.map(WeatherModel::new)
//                .map(WeatherModel::getTemperature_day).reduce(Double::sum)
//                .map(value -> value / count);
//        map.print();
        //
        MapOperator<WeatherModel, String> day_weather = dataSource.map(value -> new WeatherModel(value)).map(WeatherModel::getWeather_bay);
        MapOperator<WeatherModel, String> night_weather = dataSource.map(value -> new WeatherModel(value)).map(WeatherModel::getWeather_night);

        day_weather.filter(value -> !value.trim().equals("多云"))
                .filter(value -> !value.trim().equals("阴"))
                .filter(value -> !value.trim().equals("晴")).reduce(((value1, value2) -> value1+value2)).print();



    }
}
