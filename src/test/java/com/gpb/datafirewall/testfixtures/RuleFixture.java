package com.gpb.datafirewall.testfixtures;
import com.gpb.datafirewall.model.Rule;
import java.util.Map;
import java.util.Set;
public final class RuleFixture implements Rule {
    @Override public boolean apply(Map<String, String> data) { return "bad".equals(data.get("field")); }
    @Override public Set<String> referencedFields() { return Set.of("field"); }
}
