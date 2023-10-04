package com.example.KafkaToSparkStreamingService.entity;

import java.io.Serializable;
import java.util.List;

import com.google.gson.Gson;

public class Restaurant implements Serializable {
    private static final long serialVersionUID = 1L;
        private String _id;
        private String name;
        private Address address;
        private String weighted_rating_value;

    public Restaurant(String id,String name, String latitude, String longitude,String rating) {
        this._id = id;
        this.name = name;
        this.address = new Address(latitude, longitude);
        this.weighted_rating_value = rating;
    }

    public static Restaurant of(String _id, String name, String latitude, String longitude,String rating) {
        return new Restaurant(_id,name,latitude,longitude,rating);
    }
    public String getWeighted_rating_value() {
        return weighted_rating_value;
    }
    public void setWeighted_rating_value(String weighted_rating_value) {
        this.weighted_rating_value = weighted_rating_value;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public static List<Restaurant> parse(String json) {
            Gson gson = new Gson();
            RestaurantsList restaurantsList = gson.fromJson(json, RestaurantsList.class);
            return restaurantsList.getRestaurants();
        }

        private static class RestaurantsList implements Serializable {
            private static final long serialVersionUID = 1L;

            private List<Restaurant> restaurants;

            public List<Restaurant> getRestaurants() {
                return restaurants;
            }
        }
    public static class Address implements Serializable{
        private static final long serialVersionUID = 1L;
        private String latitude;
        private String longitude;

        public Address(String latitude, String longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public String getLatitude() {
            return latitude;
        }

        public void setLatitude(String latitude) {
            this.latitude = latitude;
        }

        public String getLongitude() {
            return longitude;
        }

        public void setLongitude(String longitude) {
            this.longitude = longitude;
        }
    }
}
