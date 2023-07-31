package com.example.utility;

import java.lang.reflect.Type;
import java.time.format.DateTimeParseException;

import org.joda.time.Instant;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

public class Gson {
    private com.google.gson.Gson gson;

    public Gson() {
        this.gson = new GsonBuilder()
                .registerTypeAdapter(Instant.class, new InstantDeserializer())
                .create();
    }

    public <T> T fromJson(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }

    public String toJson(Object object) {
        return gson.toJson(object);
    }

    private static class InstantDeserializer implements JsonDeserializer<Instant> {
        @Override
        public Instant deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            String timestampStr = json.getAsString();
            try {
                return Instant.parse(timestampStr);
            } catch (DateTimeParseException e) {
                throw new JsonParseException("Error parsing Instant from the timestamp: " + timestampStr, e);
            }
        }
    }
}
