package com.zhang.flink.epidemic;


import com.alibaba.fastjson.JSON;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.chrono.ChronoLocalDateTime;
import java.time.format.DateTimeFormatter;


@Slf4j

public class EpidemicModel {
    private final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private String continentName;
    private String continentEnglishName;
    private String countryName;
    private String countryEnglishName;
    private String provinceName;
    private String provinceEnglishName;
    private Integer province_zipCode;
    private Integer province_confirmedCount;
    private Integer province_suspectedCount;
    private Integer province_curedCount;
    private Integer province_deadCount;
    private LocalDateTime updateTime;
    private String cityName;
    private String cityEnglishName;
    private Integer city_zipCode;
    private Integer city_confirmedCount;
    private Integer city_suspectedCount;
    private Integer city_curedCount;
    private Integer city_deadCount;

    public EpidemicModel() {
    }
    public EpidemicModel(Tuple19<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String> data) throws ParseException {
        this.continentName=data.f0;
        this.countryName=data.f2;
        this.continentEnglishName=data.f1;
        this.countryEnglishName=data.f3;
        this.provinceName=data.f4;
        this.provinceEnglishName=data.f5;
        this.province_zipCode= Integer.valueOf(data.f6);
        this.province_confirmedCount= Integer.valueOf(data.f7);
        this.province_suspectedCount= Integer.valueOf(data.f8);
        this.province_curedCount= Integer.valueOf(data.f9);
        this.province_deadCount= Integer.valueOf(data.f10);
        this.updateTime=LocalDateTime.parse(data.f11,dateFormat);
        this.cityName=data.f12;
        this.cityEnglishName = data.f13;
        this.city_zipCode= Integer.valueOf(data.f14);
        this.city_confirmedCount= Integer.valueOf(data.f15);
        this.city_suspectedCount= Integer.valueOf(data.f16);
        this.city_curedCount= Integer.valueOf(data.f17);
        this.city_deadCount= Integer.valueOf(data.f18);

    }

    @Override
    public String toString() {
        return "EpidemicModel{" +
                "continentName='" + continentName + '\'' +
                ", continentEnglishName='" + continentEnglishName + '\'' +
                ", countryName='" + countryName + '\'' +
                ", countryEnglishName='" + countryEnglishName + '\'' +
                ", provinceName='" + provinceName + '\'' +
                ", provinceEnglishName='" + provinceEnglishName + '\'' +
                ", province_zipCode=" + province_zipCode +
                ", province_confirmedCount=" + province_confirmedCount +
                ", province_suspectedCount=" + province_suspectedCount +
                ", province_curedCount=" + province_curedCount +
                ", province_deadCount=" + province_deadCount +
                ", updateTime=" + updateTime +
                ", cityName='" + cityName + '\'' +
                ", cityEnglishName='" + cityEnglishName + '\'' +
                ", city_zipCode=" + city_zipCode +
                ", city_confirmedCount=" + city_confirmedCount +
                ", city_suspectedCount=" + city_suspectedCount +
                ", city_curedCount=" + city_curedCount +
                ", city_deadCount=" + city_deadCount +
                '}';
    }

    public String getContinentName() {
        return continentName;
    }

    public void setContinentName(String continentName) {
        this.continentName = continentName;
    }

    public String getContinentEnglishName() {
        return continentEnglishName;
    }

    public void setContinentEnglishName(String continentEnglishName) {
        this.continentEnglishName = continentEnglishName;
    }

    public String getCountryName() {
        return countryName;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public String getCountryEnglishName() {
        return countryEnglishName;
    }

    public void setCountryEnglishName(String countryEnglishName) {
        this.countryEnglishName = countryEnglishName;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }

    public String getProvinceEnglishName() {
        return provinceEnglishName;
    }

    public void setProvinceEnglishName(String provinceEnglishName) {
        this.provinceEnglishName = provinceEnglishName;
    }

    public Integer getProvince_zipCode() {
        return province_zipCode;
    }

    public void setProvince_zipCode(Integer province_zipCode) {
        this.province_zipCode = province_zipCode;
    }

    public Integer getProvince_confirmedCount() {
        return province_confirmedCount;
    }

    public void setProvince_confirmedCount(Integer province_confirmedCount) {
        this.province_confirmedCount = province_confirmedCount;
    }

    public Integer getProvince_suspectedCount() {
        return province_suspectedCount;
    }

    public void setProvince_suspectedCount(Integer province_suspectedCount) {
        this.province_suspectedCount = province_suspectedCount;
    }

    public Integer getProvince_curedCount() {
        return province_curedCount;
    }

    public void setProvince_curedCount(Integer province_curedCount) {
        this.province_curedCount = province_curedCount;
    }

    public Integer getProvince_deadCount() {
        return province_deadCount;
    }

    public void setProvince_deadCount(Integer province_deadCount) {
        this.province_deadCount = province_deadCount;
    }

    public ChronoLocalDateTime<LocalDate> getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getCityEnglishName() {
        return cityEnglishName;
    }

    public void setCityEnglishName(String cityEnglishName) {
        this.cityEnglishName = cityEnglishName;
    }

    public Integer getCity_zipCode() {
        return city_zipCode;
    }

    public void setCity_zipCode(Integer city_zipCode) {
        this.city_zipCode = city_zipCode;
    }

    public Integer getCity_confirmedCount() {
        return city_confirmedCount;
    }

    public void setCity_confirmedCount(Integer city_confirmedCount) {
        this.city_confirmedCount = city_confirmedCount;
    }

    public Integer getCity_suspectedCount() {
        return city_suspectedCount;
    }

    public void setCity_suspectedCount(Integer city_suspectedCount) {
        this.city_suspectedCount = city_suspectedCount;
    }

    public Integer getCity_curedCount() {
        return city_curedCount;
    }

    public void setCity_curedCount(Integer city_curedCount) {
        this.city_curedCount = city_curedCount;
    }

    public Integer getCity_deadCount() {
        return city_deadCount;
    }

    public void setCity_deadCount(Integer city_deadCount) {
        this.city_deadCount = city_deadCount;
    }

    public long getTime() {
        return updateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static class JSONSerializer implements Serializer<EpidemicModel> {
        @Override
        public byte[] serialize(String epidemic, EpidemicModel data) {
            String string = JSON.toJSONString(data);
            log.info(string);
            return string.getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class EpidemicRecord implements KafkaRecordDeserializationSchema<EpidemicModel> {
        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EpidemicModel> out) throws IOException {
            String s = new String(record.value(), StandardCharsets.UTF_8);
            EpidemicModel epidemicModel = JSON.parseObject(s, EpidemicModel.class);
//            log.info(String.valueOf(epidemicModel));
            out.collect(epidemicModel);

        }

        @Override
        public TypeInformation<EpidemicModel> getProducedType() {
            return TypeInformation.of(EpidemicModel.class);
        }
    }
}
