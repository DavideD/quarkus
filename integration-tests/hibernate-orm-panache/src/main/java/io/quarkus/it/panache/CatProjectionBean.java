package io.quarkus.it.panache;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class CatProjectionBean {

    private final String name;

    private final String ownerName;

    public CatProjectionBean(String name, String ownerName) {
        this.name = name;
        this.ownerName = ownerName;
    }

    public String getName() {
        return name;
    }

    public String getOwnerName() {
        return ownerName;
    }
}
