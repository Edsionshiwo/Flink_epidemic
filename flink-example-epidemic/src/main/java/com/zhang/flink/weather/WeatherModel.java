package com.zhang.flink.weather;

import org.apache.flink.api.java.tuple.Tuple4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WeatherModel {
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy年MM月dd日");
    private final String pattern = "-?[0-9]\\d*\\.?\\d*";
    private final Pattern r = Pattern.compile(pattern);
    private Date data;
    private Double temperature_day;
    private Double temperature_night;
    private String wind_day;
    private String wind_night;
    private String weather_bay;
    private String weather_night;

    public WeatherModel() {
    }

    public WeatherModel(Tuple4<String, String, String, String> data) throws ParseException {
        //时间
        this.data = format.parse(data.f0);
        String[] temperature = data.f1.split("/");
        //白天
        Matcher m = r.matcher(temperature[0]);
        this.temperature_day = m.find() ? Double.valueOf(m.group()) : null;
        //晚上
        Matcher ym = r.matcher(temperature[1]);
        this.temperature_night = ym.find() ? Double.valueOf(m.group()) : null;
        //风向
        String[] wind = data.f2.split("/");
        this.wind_day=wind[0];
        this.wind_night=wind[1];
        //天气
        String[] weather = data.f3.split("/");
        this.weather_bay=weather[0];
        this.weather_night = weather[1];
    }

    @Override
    public String toString() {
        return "WeatherModel{" +
                "data=" + format.format(data) +
                ", temperature_day=" + temperature_day +
                ", temperature_night=" + temperature_night +
                ", wind_day=" + wind_day +
                ", wind_night=" + wind_night +
                ", weather_bay=" + weather_bay +
                ", weather_night=" + weather_night +
                '}';
    }


    public Date getData() {
        return data;
    }

    public void setData(Date data) {
        this.data = data;
    }

    public Double getTemperature_day() {
        return temperature_day;
    }

    public void setTemperature_day(Double temperature_day) {
        this.temperature_day = temperature_day;
    }

    public Double getTemperature_night() {
        return temperature_night;
    }

    public void setTemperature_night(Double temperature_night) {
        this.temperature_night = temperature_night;
    }

    public String getWind_day() {
        return wind_day;
    }

    public void setWind_day(String wind_day) {
        this.wind_day = wind_day;
    }

    public String getWind_night() {
        return wind_night;
    }

    public void setWind_night(String wind_night) {
        this.wind_night = wind_night;
    }

    public String getWeather_bay() {
        return weather_bay;
    }

    public void setWeather_bay(String weather_bay) {
        this.weather_bay = weather_bay;
    }

    public String getWeather_night() {
        return weather_night;
    }

    public void setWeather_night(String weather_night) {
        this.weather_night = weather_night;
    }
}
