package com.twq.practice;

import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class NcdcStationMetadataParser {


    public static String getStationId(String line) {
        String[] values = line.split(",");
        if ("USAF".equals(values[0]) || values.length != 11) { // header
            return null;
        }
        return values[0].replace("\"", "") + "-" + values[1].replace("\"", "");
    }

    public static StationInfoDto fromLine(String line) {

        String[] values = line.split(",");

        StationInfoDto record = new StationInfoDto();
        record.setStationId(values[0].replace("\"", "") + "-" + values[1].replace("\"", ""));
        record.setStationName(values[2].replace("\"", ""));
        record.setCity(values[3].replace("\"", ""));
        record.setState(values[4].replace("\"", ""));
        record.setICAO(values[5].replace("\"", ""));
        record.setLatitude(values[6].replace("\"", ""));
        record.setLongitude(values[7].replace("\"", ""));
        record.setElev(values[8].replace("\"", ""));
        record.setBeginTime(values[9].replace("\"", ""));
        record.setEndTime(values[10].replace("\"", ""));

        return record;
    }

    public static StationInfoDto fromLine(Text record) {
        return fromLine(record.toString());
    }

    public static void main(String[] args) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("src/main/resources/isd-history.csv")));

        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            fromLine(line);
        }
    }


}
