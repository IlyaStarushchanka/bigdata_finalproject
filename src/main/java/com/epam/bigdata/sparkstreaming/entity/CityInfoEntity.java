package com.epam.bigdata.sparkstreaming.entity;

/**
 * Created by Ilya_Starushchanka on 10/24/2016.
 */
public class CityInfoEntity {


    private float latitude;
    private float longitude;

    public CityInfoEntity(){}

    public CityInfoEntity(float latitude, float longitude){
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

}
