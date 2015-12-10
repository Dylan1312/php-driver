#ifndef PHP_CASSANDRA_UTIL_COLLECTIONS_H
#define PHP_CASSANDRA_UTIL_COLLECTIONS_H

int php_cassandra_value_type(char* type, CassValueType* value_type TSRMLS_DC);
int php_cassandra_validate_object(zval* object, zval* ztype, zval** output TSRMLS_DC);

unsigned php_cassandra_value_hash(const zval* zvalue TSRMLS_DC);
int php_cassandra_value_compare(const zval* zvalue1, const zval* zvalue2 TSRMLS_DC);

const char* php_cassandra_type_name(CassValueType value_type);

int php_cassandra_collection_from_set(cassandra_set* set, CassCollection** collection_ptr TSRMLS_DC);
int php_cassandra_collection_from_collection(cassandra_collection* coll, CassCollection** collection_ptr TSRMLS_DC);
int php_cassandra_collection_from_map(cassandra_map* map, CassCollection** collection_ptr TSRMLS_DC);

#endif /* PHP_CASSANDRA_UTIL_COLLECTIONS_H */
