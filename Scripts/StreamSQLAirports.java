package org.masterinformatica.streaming;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

public class StreamSQLAirports {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: StreamSQLAirports <streaming directory>");
            System.exit(1);
        }
        String path = args[0];

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);

        CsvTableSource tableSource = CsvTableSource.builder()
                .path(path)
                .field("id", Types.INT)
                .field("ident",Types.STRING)
                .field("type",Types.STRING)
                .field("name",Types.STRING)
                .field("latitude_deg",Types.DOUBLE)
                .field("longitude_deg",Types.DOUBLE)
                .field("elevation_ft",Types.STRING)
                .field("continent",Types.STRING)
                .field("iso_country",Types.STRING)
                .field("iso_region",Types.STRING)
                .field("municipality",Types.STRING)
                .field("scheduled_service",Types.STRING)
                .field("gps_code",Types.STRING)
                .field("iata_code",Types.STRING)
                .field("local_code",Types.STRING)
                .field("home_link",Types.STRING)
                .field("wikipedia_link",Types.STRING)
                .field("keywords",Types.STRING)
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .ignoreParseErrors()
                .lineDelimiter("\n").build();
        tEnv.registerTableSource("airport", tableSource);
        Table table = tEnv.scan("airport");
        table.printSchema();


        Table result = tEnv.sqlQuery("SELECT * FROM " + table + " WHERE iso_country like '%ES%'");
        tEnv.toAppendStream(result, Airport.class).print();

        env.execute("Airport");
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /**
     * Simple POJO.
     */
    public static class Airport {
        private int id;
        private  String ident;
        private  String type;
        private  String name;
        private double latitude_deg;
        private  double longitude_deg;
        private  String elevation_ft;
        private  String continent;
        private  String iso_country;
        private  String iso_region;
        private  String municipality;
        private  String scheduled_service;
        private  String gps_code;
        private  String iata_code;
        private  String local_code;
        private  String home_link;
        private  String wikipedia_link;
        private  String keywords;

        public Airport(){

        }

        public Airport(int id, String ident, String type, String name, double latitude_deg, double longitude_deg, String elevation_ft, String continent, String iso_country, String iso_region, String municipality, String scheduled_service, String gps_code, String iata_code, String local_code, String home_link, String wikipedia_link, String keywords) {
            this.id = id;
            this.ident = ident;
            this.type = type;
            this.name = name;
            this.latitude_deg = latitude_deg;
            this.longitude_deg = longitude_deg;
            this.elevation_ft = elevation_ft;
            this.continent = continent;
            this.iso_country = iso_country;
            this.iso_region = iso_region;
            this.municipality = municipality;
            this.scheduled_service = scheduled_service;
            this.gps_code = gps_code;
            this.iata_code = iata_code;
            this.local_code = local_code;
            this.home_link = home_link;
            this.wikipedia_link = wikipedia_link;
            this.keywords = keywords;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getIdent() {
            return ident;
        }

        public void setIdent(String ident) {
            this.ident = ident;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getLatitude_deg() {
            return latitude_deg;
        }

        public void setLatitude_deg(double latitude_deg) {
            this.latitude_deg = latitude_deg;
        }

        public double getLongitude_deg() {
            return longitude_deg;
        }

        public void setLongitude_deg(double longitude_deg) {
            this.longitude_deg = longitude_deg;
        }

        public String getElevation_ft() {
            return elevation_ft;
        }

        public void setElevation_ft(String elevation_ft) {
            this.elevation_ft = elevation_ft;
        }

        public String getContinent() {
            return continent;
        }

        public void setContinent(String continent) {
            this.continent = continent;
        }

        public String getIso_country() {
            return iso_country;
        }

        public void setIso_country(String iso_country) {
            this.iso_country = iso_country;
        }

        public String getIso_region() {
            return iso_region;
        }

        public void setIso_region(String iso_region) {
            this.iso_region = iso_region;
        }

        public String getMunicipality() {
            return municipality;
        }

        public void setMunicipality(String municipality) {
            this.municipality = municipality;
        }

        public String getScheduled_service() {
            return scheduled_service;
        }

        public void setScheduled_service(String scheduled_service) {
            this.scheduled_service = scheduled_service;
        }

        public String getGps_code() {
            return gps_code;
        }

        public void setGps_code(String gps_code) {
            this.gps_code = gps_code;
        }

        public String getIata_code() {
            return iata_code;
        }

        public void setIata_code(String iata_code) {
            this.iata_code = iata_code;
        }

        public String getLocal_code() {
            return local_code;
        }

        public void setLocal_code(String local_code) {
            this.local_code = local_code;
        }

        public String getHome_link() {
            return home_link;
        }

        public void setHome_link(String home_link) {
            this.home_link = home_link;
        }

        public String getWikipedia_link() {
            return wikipedia_link;
        }

        public void setWikipedia_link(String wikipedia_link) {
            this.wikipedia_link = wikipedia_link;
        }

        public String getKeywords() {
            return keywords;
        }

        public void setKeywords(String keywords) {
            this.keywords = keywords;
        }

        @Override
        public String toString() {
            return "Airport \n"+
                   " ID: " + getId()+"\n"+
            "ident: " +getIdent()+"\n"+
            "type: " + getType()+"\n"+
            "name " +getName()+"\n"+
            "latitude_deg: "+ getLatitude_deg()+"\n"+
            "longitude_deg: " +getLongitude_deg()+"\n"+
            "elevation_ft: " +getElevation_ft()+"\n"+
            "continent: " + getContinent()+"\n"+
            "iso_country: " + getIso_country()+"\n"+
            "iso_region: " +getIso_region()+"\n"+
            "municipality: " + getMunicipality()+"\n"+
            "scheduled_service: " +getScheduled_service()+"\n"+
            "gps_code: "+getGps_code()+"\n"+
            "iata_code: " + getIata_code()+"\n"+
            "local_code: " +getLocal_code()+"\n"+
            "home_link: " + getHome_link()+"\n"+
            "wikipedia_link: " + getWikipedia_link()+"\n"+
            "keywords: "+getKeywords()+"\n";
        }
    }

}
