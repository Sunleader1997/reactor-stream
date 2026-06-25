package io.github.sunleader1997.reactorstream.database;

import java.io.Serializable;
import java.util.UUID;

public interface AbstractData {
    public Serializable getId();

    public abstract void setId(Serializable id);

    public default void generateIdIfEmpty() {
        if (getId() == null || getId().toString().isEmpty()) {
            this.setId(UUID.randomUUID().toString());
        }
    }
}
