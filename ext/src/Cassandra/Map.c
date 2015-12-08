#include "php_cassandra.h"
#include "util/collections.h"
#include "Map.h"

zend_class_entry *cassandra_map_ce = NULL;

int
php_cassandra_map_set(cassandra_map* map, zval* zkey, zval* zvalue TSRMLS_DC)
{
  cassandra_map_entry* entry;
  int result = 0;

  if (Z_TYPE_P(zkey) == IS_NULL) {
    zend_throw_exception_ex(cassandra_invalid_argument_exception_ce, 0 TSRMLS_CC,
                            "Invalid key: null is not supported inside maps");
    return 0;
  }

  if (Z_TYPE_P(zvalue) == IS_NULL) {
    zend_throw_exception_ex(cassandra_invalid_argument_exception_ce, 0 TSRMLS_CC,
                            "Invalid value: null is not supported inside maps");
    return 0;
  }

  if (!php_cassandra_validate_object(zkey, map->key_type TSRMLS_CC)) {
    return 0;
  }

  if (!php_cassandra_validate_object(zvalue, map->value_type TSRMLS_CC)) {
    return 0;
  }

  map->dirty = 1;
  HASH_FIND_PTR(map->entries, &zkey, entry);
  if (entry == NULL) {
    entry = (cassandra_map_entry*)emalloc(sizeof(cassandra_map_entry));
    entry->key = zkey;
    entry->value = NULL;
    Z_ADDREF_P(zkey);
    HASH_ADD_PTR(map->entries, key, entry);
  } else {
    zval_ptr_dtor(&entry->value);
  }

  if (zvalue != entry->value) {
    Z_ADDREF_P(zvalue);
    entry->value = zvalue;
  }

  return result;
}

static int
php_cassandra_map_get(cassandra_map* map, zval* zkey, zval** zvalue TSRMLS_DC)
{
  cassandra_map_entry* entry;
  int result = 0;

  if (!php_cassandra_validate_object(zkey, map->key_type TSRMLS_CC)) {
    return 0;
  }

  HASH_FIND_PTR(map->entries, &zkey, entry);
  if (entry != NULL) {
    *zvalue = entry->value;
    result = 1;
  }

  return result;
}

static int
php_cassandra_map_del(cassandra_map* map, zval* zkey TSRMLS_DC)
{
  cassandra_map_entry* entry;
  int result = 0;

  if (!php_cassandra_validate_object(zkey, map->key_type TSRMLS_CC)) {
    return 0;
  }

  HASH_FIND_PTR(map->entries, &zkey, entry);
  if (entry != NULL) {
    map->dirty = 1;
    if (entry == map->iter_temp) {
      map->iter_temp = (cassandra_map_entry*)map->iter_temp->hh.next;
    }
    HASH_DEL(map->entries, entry);
    zval_ptr_dtor(&entry->key);
    zval_ptr_dtor(&entry->value);
    efree(entry);
    result = 1;
  }

  return result;
}

static int
php_cassandra_map_has(cassandra_map* map, zval* zkey TSRMLS_DC)
{
  cassandra_map_entry* entry;
  int result = 0;

  if (!php_cassandra_validate_object(zkey, map->key_type TSRMLS_CC)) {
    return 0;
  }

  HASH_FIND_PTR(map->entries, &zkey, entry);
  if (entry != NULL) {
    result = 1;
  }

  return result;
}

static void
php_cassandra_map_populate_keys(const cassandra_map* map, zval* array)
{
  cassandra_map_entry* curr, * temp;
  HASH_ITER(hh, map->entries, curr, temp) {
    if (add_next_index_zval(array, curr->key) != SUCCESS) {
      break;
    }
    Z_ADDREF_P(curr->key);
  }
}

static void
php_cassandra_map_populate_values(const cassandra_map* map, zval* array)
{
  cassandra_map_entry* curr, * temp;
  HASH_ITER(hh, map->entries, curr, temp) {
    if (add_next_index_zval(array, curr->value) != SUCCESS) {
      break;
    }
    Z_ADDREF_P(curr->value);
  }
}

/* {{{ Cassandra\Map::__construct(string, string) */
PHP_METHOD(Map, __construct)
{
  char* key_type;
  char* value_type;
  int key_type_len, value_type_len;
  cassandra_map* map;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss", &key_type, &key_type_len, &value_type, &value_type_len) == FAILURE) {
    return;
  }

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (!php_cassandra_value_type(key_type, &map->key_type TSRMLS_CC))
    return;
  php_cassandra_value_type(value_type, &map->value_type TSRMLS_CC);
}
/* }}} */

/* {{{ Cassandra\Map::keyType() */
PHP_METHOD(Map, keyType)
{
  cassandra_map* map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  RETURN_STRING(php_cassandra_type_name(map->key_type), 1);
}
/* }}} */

/* {{{ Cassandra\Map::valueType() */
PHP_METHOD(Map, valueType)
{
  cassandra_map* map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  RETURN_STRING(php_cassandra_type_name(map->value_type), 1);
}
/* }}} */

PHP_METHOD(Map, keys)
{
  cassandra_map* map = NULL;
  array_init(return_value);
  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);
  php_cassandra_map_populate_keys(map, return_value);
}

PHP_METHOD(Map, values)
{
  cassandra_map* map = NULL;
  array_init(return_value);
  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);
  php_cassandra_map_populate_values(map, return_value);
}

PHP_METHOD(Map, set)
{
  zval* key;
  cassandra_map* map = NULL;
  zval* value;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "zz", &key, &value) == FAILURE)
    return;

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_map_set(map, key, value TSRMLS_CC))
    RETURN_TRUE;

  RETURN_FALSE;
}

PHP_METHOD(Map, get)
{
  zval* key;
  cassandra_map* map = NULL;
  zval* value;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &key) == FAILURE)
    return;

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_map_get(map, key, &value TSRMLS_CC))
    RETURN_ZVAL(value, 1, 0);
}

PHP_METHOD(Map, remove)
{
  zval* key;
  cassandra_map* map = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &key) == FAILURE)
    return;

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_map_del(map, key TSRMLS_CC))
    RETURN_TRUE;

  RETURN_FALSE;
}

PHP_METHOD(Map, has)
{
  zval* key;
  cassandra_map* map = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &key) == FAILURE)
    return;

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_map_has(map, key TSRMLS_CC))
    RETURN_TRUE;

  RETURN_FALSE;
}

PHP_METHOD(Map, count)
{
  cassandra_map* map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_LONG((long)HASH_COUNT(map->entries));
}

PHP_METHOD(Map, current)
{
  cassandra_map* map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);
  if (map->iter_curr != NULL)
    RETURN_ZVAL(map->iter_curr->value, 1, 0);
}

PHP_METHOD(Map, key)
{
  cassandra_map* map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);
  if (map->iter_curr != NULL)
    RETURN_ZVAL(map->iter_curr->key, 1, 0);
}

PHP_METHOD(Map, next)
{
  cassandra_map* map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);
  map->iter_curr = map->iter_temp;
  map->iter_temp = map->iter_temp != NULL ? (cassandra_map_entry*)map->iter_temp->hh.next : NULL;
}

PHP_METHOD(Map, valid)
{
  cassandra_map* map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_BOOL(map->iter_curr != NULL);
}

PHP_METHOD(Map, rewind)
{
  cassandra_map* map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);
  map->iter_curr = map->entries;
  map->iter_temp = map->entries != NULL ? (cassandra_map_entry*)map->entries->hh.next : NULL;
}

PHP_METHOD(Map, offsetSet)
{
  zval* key;
  cassandra_map* map = NULL;
  zval* value;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "zz", &key, &value) == FAILURE)
    return;

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  php_cassandra_map_set(map, key, value TSRMLS_CC);
}

PHP_METHOD(Map, offsetGet)
{
  zval* key;
  cassandra_map* map = NULL;
  zval* value;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &key) == FAILURE)
    return;

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_map_get(map, key, &value TSRMLS_CC))
    RETURN_ZVAL(value, 1, 0);
}

PHP_METHOD(Map, offsetUnset)
{
  zval* key;
  cassandra_map* map = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &key) == FAILURE)
    return;

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  php_cassandra_map_del(map, key TSRMLS_CC);
}

PHP_METHOD(Map, offsetExists)
{
  zval* key;
  cassandra_map* map = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &key) == FAILURE)
    return;

  map = (cassandra_map*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_map_has(map, key TSRMLS_CC))
    RETURN_TRUE;

  RETURN_FALSE;
}

ZEND_BEGIN_ARG_INFO_EX(arginfo__construct, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, type)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_two, 0, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_one, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_none, 0, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_map_methods[] = {
  PHP_ME(Map, __construct, arginfo__construct, ZEND_ACC_CTOR|ZEND_ACC_PUBLIC)
  PHP_ME(Map, keyType, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Map, keys, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Map, valueType, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Map, values, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Map, set, arginfo_two, ZEND_ACC_PUBLIC)
  PHP_ME(Map, get, arginfo_one, ZEND_ACC_PUBLIC)
  PHP_ME(Map, remove, arginfo_one, ZEND_ACC_PUBLIC)
  PHP_ME(Map, has, arginfo_one, ZEND_ACC_PUBLIC)
  /* Countable */
  PHP_ME(Map, count, arginfo_none, ZEND_ACC_PUBLIC)
  /* Iterator */
  PHP_ME(Map, current, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Map, key, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Map, next, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Map, valid, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Map, rewind, arginfo_none, ZEND_ACC_PUBLIC)
  /* ArrayAccess */
  PHP_ME(Map, offsetSet, arginfo_two, ZEND_ACC_PUBLIC)
  PHP_ME(Map, offsetGet, arginfo_one, ZEND_ACC_PUBLIC)
  PHP_ME(Map, offsetUnset, arginfo_one, ZEND_ACC_PUBLIC)
  PHP_ME(Map, offsetExists, arginfo_one, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_map_handlers;

static HashTable*
php_cassandra_map_gc(zval *object, zval ***table, int *n TSRMLS_DC)
{
  *table = NULL;
  *n = 0;
  return zend_std_get_properties(object TSRMLS_CC);
}

static HashTable*
php_cassandra_map_properties(zval *object TSRMLS_DC)
{
  zval* values;
  zval* keys;

  cassandra_map* map   = (cassandra_map*) zend_object_store_get_object(object TSRMLS_CC);
  HashTable*     props = zend_std_get_properties(object TSRMLS_CC);

  MAKE_STD_ZVAL(values);
  MAKE_STD_ZVAL(keys);
  array_init(values);
  array_init(keys);
  php_cassandra_map_populate_values(map, values);
  php_cassandra_map_populate_keys(map, keys);
  zend_hash_update(props, "keys", sizeof("keys"), &keys, sizeof(zval*), NULL);
  zend_hash_update(props, "values", sizeof("values"), &values, sizeof(zval*), NULL);

  return props;
}

static int
php_cassandra_map_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  if (php_cassandra_value_compare(obj1, obj2 TSRMLS_CC) == 0)
    return 0;

  return 1;
}

static void
php_cassandra_map_free(void *object TSRMLS_DC)
{
  cassandra_map* map = (cassandra_map*) object;
  cassandra_map_entry* curr, * temp;

  HASH_ITER(hh, map->entries, curr, temp) {
    zval_ptr_dtor(&curr->key);
    zval_ptr_dtor(&curr->value);
    HASH_DEL(map->entries, curr);
    efree(curr);
  }
  zend_object_std_dtor(&map->zval TSRMLS_CC);

  efree(map);
}

static zend_object_value
php_cassandra_map_new(zend_class_entry* class_type TSRMLS_DC)
{
  zend_object_value retval;
  cassandra_map *map;

  map = (cassandra_map*) emalloc(sizeof(cassandra_map));
  memset(map, 0, sizeof(cassandra_map));

  map->type = CASS_VALUE_TYPE_MAP;
  map->entries = map->iter_curr = map->iter_temp = NULL;
  map->dirty = 1;

  zend_object_std_init(&map->zval, class_type TSRMLS_CC);
  object_properties_init(&map->zval, class_type);

  retval.handle   = zend_objects_store_put(map,
                      (zend_objects_store_dtor_t) zend_objects_destroy_object,
                      php_cassandra_map_free, NULL TSRMLS_CC);
  retval.handlers = &cassandra_map_handlers;

  return retval;
}

void cassandra_define_Map(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Map", cassandra_map_methods);
  cassandra_map_ce = zend_register_internal_class(&ce TSRMLS_CC);
  memcpy(&cassandra_map_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_map_handlers.get_properties  = php_cassandra_map_properties;
#if PHP_VERSION_ID >= 50400
  cassandra_map_handlers.get_gc          = php_cassandra_map_gc;
#endif
  cassandra_map_handlers.compare_objects = php_cassandra_map_compare;
  cassandra_map_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
  cassandra_map_ce->create_object = php_cassandra_map_new;
  zend_class_implements(cassandra_map_ce TSRMLS_CC, 3, spl_ce_Countable, zend_ce_iterator, zend_ce_arrayaccess);
}
