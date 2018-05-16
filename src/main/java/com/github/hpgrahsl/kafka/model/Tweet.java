package com.github.hpgrahsl.kafka.model;

public class Tweet {

    public long Id;
    public long CreatedAt;
    public String Text;
    public String Lang;

    @Override
    public String toString() {
        return "Tweet{" +
            "Id=" + Id +
            ", CreatedAt=" + CreatedAt +
            ", Text='" + Text + '\'' +
            ", Lang='" + Lang + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tweet tweet = (Tweet) o;

        return Id == tweet.Id;
    }

    @Override
    public int hashCode() {
        return (int) (Id ^ (Id >>> 32));
    }

}
