package edu.big.data;

import java.time.LocalDate;
import java.util.Objects;

public class UserArticleDateBehavior {
    private String uid;

    private String domain;

    private String behavior;

    private LocalDate behaviorDate;

    private Integer counts;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public LocalDate getBehaviorDate() {
        return behaviorDate;
    }

    public void setBehaviorDate(LocalDate behaviorDate) {
        this.behaviorDate = behaviorDate;
    }

    public Integer getCounts() {
        return counts;
    }

    public void setCounts(Integer counts) {
        this.counts = counts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserArticleDateBehavior that = (UserArticleDateBehavior) o;
        return Objects.equals(uid, that.uid) &&
                Objects.equals(domain, that.domain) &&
                Objects.equals(behavior, that.behavior) &&
                Objects.equals(behaviorDate, that.behaviorDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uid, domain, behavior, behaviorDate, counts);
    }
}
