// ============================================================================
// Painless Script - Remove Empty Values from Nested Structures
// Uses post-order traversal to recursively clean Maps and Lists
// ============================================================================

/**
 * Check if a value is considered empty
 */
boolean isEmpty(def value) {
    if (value == null) {
        return true;
    }
    if (value instanceof String) {
        return value.trim().length() == 0;
    }
    if (value instanceof List) {
        return ((List)value).isEmpty();
    }
    if (value instanceof Map) {
        return ((Map)value).isEmpty();
    }
    return false;
}

/**
 * Remove empty values from a Map
 */
void cleanMap(Map map) {
    def keysToRemove = new ArrayList();
    
    for (def key : map.keySet()) {
        def val = map[key];
        if (isEmpty(val)) {
            keysToRemove.add(key);
        }
    }
    
    for (def key : keysToRemove) {
        map.remove(key);
    }
}

/**
 * Remove empty values from a List (iterate in reverse to avoid index issues)
 */
void cleanList(List list) {
    for (int i = list.size() - 1; i >= 0; i--) {
        def element = list.get(i);
        if (isEmpty(element)) {
            list.remove(i);
        }
    }
}

/**
 * Post-order traversal to recursively clean all nested structures
 * Stack-based approach to avoid recursion depth issues
 */
def vals = new ArrayList();
def stages = new ArrayList();

vals.add(ctx);
stages.add(0);

while (vals.size() > 0) {
    def currentValue = vals.remove(vals.size() - 1);
    int stage = stages.remove(stages.size() - 1);
    
    // Stage 0: Push children onto stack
    if (stage == 0) {
        if (currentValue instanceof Map) {
            // Push map for stage 1 processing
            vals.add(currentValue);
            stages.add(1);
            
            // Push all values for stage 0 processing
            def keys = new ArrayList(((Map)currentValue).keySet());
            for (def k : keys) {
                vals.add(((Map)currentValue)[k]);
                stages.add(0);
            }
        } else if (currentValue instanceof List) {
            // Push list for stage 1 processing
            vals.add(currentValue);
            stages.add(1);
            
            // Push all elements for stage 0 processing
            for (int i = 0; i < ((List)currentValue).size(); i++) {
                vals.add(((List)currentValue).get(i));
                stages.add(0);
            }
        }
    } 
    // Stage 1: Process after children are cleaned
    else {
        if (currentValue instanceof Map) {
            cleanMap((Map)currentValue);
        } else if (currentValue instanceof List) {
            cleanList((List)currentValue);
        }
    }
}
