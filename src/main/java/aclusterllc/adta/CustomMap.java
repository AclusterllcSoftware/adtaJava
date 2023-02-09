package aclusterllc.adta;

import java.util.HashMap;
import java.util.Map;

class CustomMap<K,V> extends HashMap<K, V> {

    Map<V,K> reverseMap = new HashMap<V,K>();

    @Override
    public V put(K key, V value) {
        // TODO Auto-generated method stub
        reverseMap.put(value, key);
        return super.put(key, value);
    }

    public K getKey(V value){
        return reverseMap.get(value);
    }
}
