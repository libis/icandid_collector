// ============================================================================
// Painless Script - Refactored for Readability
// Processes prov:wasAttributedTo and prov:wasAssociatedFor data,
// accumulating oversized values based on string length (max 8191 chars)
// ============================================================================

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Check if combined string length exceeds maximum
 * 16 bytes overhead accounts for JSON structure
 */
boolean isOversized(int keyLength, int valueLength, int maxStrLength) {
    return (keyLength + valueLength + 4) > maxStrLength;
}

/**
 * Get a list of association maps from root object
 */
List extractAssociations(def root) {
    def assocList = new ArrayList();
    
    if (root == null) return assocList;
    
    if (root instanceof List) {
        for (def item : root) {
            if (item instanceof Map) {
                addAssociations(assocList, item['prov:wasAssociatedFor']);
            }
        }
    } else if (root instanceof Map) {
        addAssociations(assocList, root['prov:wasAssociatedFor']);
    }
    
    return assocList;
}

/**
 * Helper to add associations to list
 */
void addAssociations(List assocList, def assoc) {
    if (assoc instanceof Map) {
        assocList.add(assoc);
    } else if (assoc instanceof List) {
        for (def subItem : assoc) {
            if (subItem instanceof Map) {
                assocList.add(subItem);
            }
        }
    }
}

/**
 * Ensure _generated_text is a list in the given map
 */
List ensureGeneratedTextList(Map assocMap) {
    def acc = assocMap['_generated_text'];
    if (acc == null) {
        acc = new ArrayList();
        assocMap['_generated_text'] = acc;
    } else if (!(acc instanceof List)) {
        acc = new ArrayList([acc]);
        assocMap['_generated_text'] = acc;
    }
    return acc;
}

/**
 * Process a string value
 */
void processStringValue(String key, String value, int keyLength, List accumulatedValues, int maxStrLength) {
    int valueLength = value.length();
    if (isOversized(keyLength, valueLength, maxStrLength)) {
        accumulatedValues.add(value);
    }
}

/**
 * Process a list value
 */
void processListValue(List valueList, int keyLength, List accumulatedValues, int maxStrLength) {
    for (def item : valueList) {
        if (item instanceof String) {
            int itemLength = ((String)item).length();
            if (isOversized(keyLength, itemLength, maxStrLength)) {
                accumulatedValues.add(item);
            }
        }
    }
}

/**
 * Process a nested map value
 */
void processNestedMapValue(String parentKey, Map nestedMap, int parentKeyLength, List accumulatedValues, int maxStrLength) {
    def nestedKeys = new ArrayList(nestedMap.keySet());
    
    for (def nk : nestedKeys) {
        String nestedKey = (nk == null) ? '' : nk.toString();
        String combinedKey = parentKey + '.' + nestedKey;
        int combinedLength = combinedKey.length();
        
        def nestedValue = nestedMap[nestedKey];
        if (nestedValue == null) continue;
        
        if (nestedValue instanceof String) {
            processStringValue(nestedKey, (String)nestedValue, combinedLength, accumulatedValues, maxStrLength);
        } else if (nestedValue instanceof List) {
            processListValue((List)nestedValue, combinedLength, accumulatedValues, maxStrLength);
        }
    }
}

/**
 * Process all key-value pairs in a map-type prov:generated structure
 */
void processMapGenerated(Map generatedMap, List accumulatedValues, int maxStrLength) {
    def keys = new ArrayList(generatedMap.keySet());
    
    for (def k : keys) {
        String keyStr = (k == null) ? '' : k.toString();
        int keyLength = keyStr.length();
        
        def value = generatedMap[keyStr];
        if (value == null) continue;
        
        if (value instanceof String) {
            processStringValue(keyStr, (String)value, keyLength, accumulatedValues, maxStrLength);
        } else if (value instanceof List) {
            processListValue((List)value, keyLength, accumulatedValues, maxStrLength);
        } else if (value instanceof Map) {
            processNestedMapValue(keyStr, (Map)value, keyLength, accumulatedValues, maxStrLength);
        }
    }
}

/**
 * Process all maps within a list-type prov:generated structure
 */
void processListGenerated(List generatedList, List accumulatedValues, int maxStrLength) {
    for (def element : generatedList) {
        if (!(element instanceof Map)) continue;
        
        def elementMap = (Map)element;
        processMapGenerated(elementMap, accumulatedValues, maxStrLength);
    }
}

// ============================================================================
// MAIN LOGIC
// ============================================================================
// UTF-8 encoding uses 1 to 4 bytes per character.
// So the worst case for a string of length n is 4 * n bytes.
// Elasticsearch has a max script string size of 32,766 bytes

int MAX_STR_LENGTH = params.containsKey('max_str_length') 
    ? Integer.parseInt(params.max_str_length.toString()) 
    : 8191;

def root = ctx['prov:wasAttributedTo'];
if (root == null) return;

def assocList = extractAssociations(root);

for (def assocMap : assocList) {
    if (assocMap == null || !(assocMap instanceof Map)) continue;
    
    def generated = assocMap['prov:generated'];
    if (generated == null) continue;
    
    // Temporarily accumulate oversized values
    List accumulatedValues = new ArrayList();
    
    if (generated instanceof Map) {
        processMapGenerated((Map)generated, accumulatedValues, MAX_STR_LENGTH);
    } else if (generated instanceof List) {
        processListGenerated((List)generated, accumulatedValues, MAX_STR_LENGTH);
    }
    
    // Only create _generated_text if there are oversized values
    if (accumulatedValues.size() > 0) {
        List generatedText = ensureGeneratedTextList(assocMap);
        generatedText.addAll(accumulatedValues);
    }
}
