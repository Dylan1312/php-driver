#include "php_cassandra.h"
#include "util/collections.h"
#include "util/types.h"
#include "src/Cassandra/Set.h"

zend_class_entry *cassandra_set_ce = NULL;

int
php_cassandra_set_add(cassandra_set* set, zval* object TSRMLS_DC)
{
  cassandra_set_entry* entry;
  cassandra_type_set* type;
  zval* element;

  if (Z_TYPE_P(object) == IS_NULL) {
    zend_throw_exception_ex(cassandra_invalid_argument_exception_ce, 0 TSRMLS_CC,
                            "Invalid key: null is not supported inside sets");
    return 0;
  }

  type = (cassandra_type_set*) zend_object_store_get_object(set->ztype TSRMLS_CC);

  if (!php_cassandra_validate_object(object, type->element_type, &element TSRMLS_CC)) {
    return 0;
  }

  set->dirty = 1;
  HASH_FIND_PTR(set->entries, &element, entry);
  if (entry == NULL) {
    entry = (cassandra_set_entry*)emalloc(sizeof(cassandra_set_entry));
    entry->element = element;
    Z_ADDREF_P(entry->element);
    HASH_ADD_PTR(set->entries, element, entry);
  } else if (entry->element != element) {
    entry->element = element;
    Z_ADDREF_P(entry->element);
  }

  return 1;
}

static int
php_cassandra_set_del(cassandra_set* set, zval* object TSRMLS_DC)
{
  cassandra_set_entry* entry;
  cassandra_type_set* type;
  zval* element;
  int result = 0;

  type = (cassandra_type_set*) zend_object_store_get_object(set->ztype TSRMLS_CC);

  if (!php_cassandra_validate_object(object, type->element_type, &element TSRMLS_CC)) {
    return 0;
  }

  HASH_FIND_PTR(set->entries, &element, entry);
  if (entry != NULL) {
    set->dirty = 1;
    if (entry == set->iter_temp) {
      set->iter_temp = (cassandra_set_entry*)set->iter_temp->hh.next;
    }
    HASH_DEL(set->entries, entry);
    zval_ptr_dtor(&entry->element);
    efree(entry);
    result = 1;
  }

  return result;
}

static int
php_cassandra_set_has(cassandra_set* set, zval* object TSRMLS_DC)
{
  cassandra_set_entry* entry;
  cassandra_type_set* type;
  zval* element;
  int result = 0;

  type = (cassandra_type_set*) zend_object_store_get_object(set->ztype TSRMLS_CC);

  if (!php_cassandra_validate_object(object, type->element_type, &element TSRMLS_CC)) {
    return 0;
  }

  HASH_FIND_PTR(set->entries, &element, entry);
  if (entry != NULL) {
    result = 1;
  }

  return result;
}

static void
php_cassandra_set_populate(cassandra_set* set, zval* array)
{
  cassandra_set_entry* curr, * temp;
  HASH_ITER(hh, set->entries, curr, temp) {
    if (add_next_index_zval(array, curr->element) != SUCCESS) {
      break;
    }
    Z_ADDREF_P(curr->element);
  }
}

/* {{{ Cassandra\Set::__construct(string) */
PHP_METHOD(Set, __construct)
{
  cassandra_set* set;
  zval* element_type;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "O",
                            &element_type, cassandra_type_ce) == FAILURE) {
    return;
  }

  set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  set->ztype = php_cassandra_type_set(element_type TSRMLS_CC);
  Z_ADDREF_P(element_type);
}
/* }}} */

/* {{{ Cassandra\Set::type() */
PHP_METHOD(Set, type)
{
  cassandra_set* self = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  cassandra_type_set* type = (cassandra_type_set*) zend_object_store_get_object(self->ztype TSRMLS_CC);
  RETURN_ZVAL(type->element_type, 1, 0);
}
/* }}} */

/* {{{ Cassandra\Set::values() */
PHP_METHOD(Set, values)
{
  cassandra_set* set = NULL;
  array_init(return_value);
  set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  php_cassandra_set_populate(set, return_value);
}
/* }}} */

/* {{{ Cassandra\Set::add(value) */
PHP_METHOD(Set, add)
{
  cassandra_set* set = NULL;

  zval* object;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &object) == FAILURE)
    return;

  set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_set_add(set, object TSRMLS_CC))
    RETURN_TRUE;

  RETURN_FALSE;
}
/* }}} */

/* {{{ Cassandra\Set::remove(value) */
PHP_METHOD(Set, remove)
{
  cassandra_set* set = NULL;

  zval* object;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &object) == FAILURE)
    return;

  set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_set_del(set, object TSRMLS_CC))
    RETURN_TRUE;

  RETURN_FALSE;
}
/* }}} */

/* {{{ Cassandra\Set::has(value) */
PHP_METHOD(Set, has)
{
  cassandra_set* set = NULL;

  zval* object;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &object) == FAILURE)
    return;

  set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (php_cassandra_set_has(set, object TSRMLS_CC))
    RETURN_TRUE;

  RETURN_FALSE;
}
/* }}} */

/* {{{ Cassandra\Set::count() */
PHP_METHOD(Set, count)
{
  cassandra_set* set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_LONG((long)HASH_COUNT(set->entries));
}
/* }}} */

/* {{{ Cassandra\Set::current() */
PHP_METHOD(Set, current)
{
  cassandra_set* set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  if (set->iter_curr != NULL)
    RETURN_ZVAL(set->iter_curr->element, 1, 0);
}
/* }}} */

/* {{{ Cassandra\Set::key() */
PHP_METHOD(Set, key)
{
  cassandra_set* set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_LONG(set->iter_index);
}
/* }}} */

/* {{{ Cassandra\Set::next() */
PHP_METHOD(Set, next)
{
  cassandra_set* set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  set->iter_curr = set->iter_temp;
  set->iter_temp = set->iter_temp != NULL ? (cassandra_set_entry*)set->iter_temp->hh.next : NULL;
  set->iter_index++;
}
/* }}} */

/* {{{ Cassandra\Set::valid() */
PHP_METHOD(Set, valid)
{
  cassandra_set* set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_BOOL(set->iter_curr != NULL);
}
/* }}} */

/* {{{ Cassandra\Set::rewind() */
PHP_METHOD(Set, rewind)
{
  cassandra_set* set = (cassandra_set*) zend_object_store_get_object(getThis() TSRMLS_CC);
  set->iter_curr = set->entries;
  set->iter_temp = set->entries != NULL ? (cassandra_set_entry*)set->entries->hh.next : NULL;
  set->iter_index = 0;
}
/* }}} */

ZEND_BEGIN_ARG_INFO_EX(arginfo__construct, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, type)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_one, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_none, 0, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_set_methods[] = {
  PHP_ME(Set, __construct, arginfo__construct, ZEND_ACC_CTOR|ZEND_ACC_PUBLIC)
  PHP_ME(Set, type, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Set, values, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Set, add, arginfo_one, ZEND_ACC_PUBLIC)
  PHP_ME(Set, remove, arginfo_one, ZEND_ACC_PUBLIC)
  /* Countable */
  PHP_ME(Set, count, arginfo_none, ZEND_ACC_PUBLIC)
  /* Iterator */
  PHP_ME(Set, current, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Set, key, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Set, next, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Set, valid, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Set, rewind, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_set_handlers;

static HashTable*
php_cassandra_set_gc(zval *object, zval ***table, int *n TSRMLS_DC)
{
  *table = NULL;
  *n = 0;
  return zend_std_get_properties(object TSRMLS_CC);
}

static HashTable*
php_cassandra_set_properties(zval *object TSRMLS_DC)
{
  zval* values;

  cassandra_set* set   = (cassandra_set*) zend_object_store_get_object(object TSRMLS_CC);
  HashTable*     props = zend_std_get_properties(object TSRMLS_CC);

  MAKE_STD_ZVAL(values);
  array_init(values);

  php_cassandra_set_populate(set, values);

  zend_hash_update(props, "values", sizeof("values"), &values, sizeof(zval), NULL);

  return props;
}

int zend_compare_symbol_tables_i(HashTable *ht1, HashTable *ht2 TSRMLS_DC);

static int
php_cassandra_set_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  return php_cassandra_value_compare(obj1, obj2 TSRMLS_CC);
}

static void
php_cassandra_set_free(void *object TSRMLS_DC)
{
  cassandra_set* set = (cassandra_set*) object;
  cassandra_set_entry* curr, * temp;

  zval_ptr_dtor(&set->ztype); /* TODO(mpenick): Move to Value dtor? */

  HASH_ITER(hh, set->entries, curr, temp) {
    zval_ptr_dtor(&curr->element);
    HASH_DEL(set->entries, curr);
    efree(curr);
  }

  zend_object_std_dtor(&set->zval TSRMLS_CC);

  efree(set);
}

static zend_object_value
php_cassandra_set_new(zend_class_entry* class_type TSRMLS_DC)
{
  zend_object_value retval;
  cassandra_set *set;

  set = (cassandra_set*) emalloc(sizeof(cassandra_set));
  memset(set, 0, sizeof(cassandra_set));

  set->type = CASS_VALUE_TYPE_SET;
  set->ztype = NULL;
  set->entries = set->iter_curr = set->iter_temp = NULL;
  set->iter_index = 0;
  set->dirty = 1;

  zend_object_std_init(&set->zval, class_type TSRMLS_CC);
  object_properties_init(&set->zval, class_type);

  retval.handle   = zend_objects_store_put(set,
                      (zend_objects_store_dtor_t) zend_objects_destroy_object,
                      php_cassandra_set_free, NULL TSRMLS_CC);
  retval.handlers = &cassandra_set_handlers;

  return retval;
}

void cassandra_define_Set(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Set", cassandra_set_methods);
  cassandra_set_ce = zend_register_internal_class(&ce TSRMLS_CC);
  memcpy(&cassandra_set_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_set_handlers.get_properties  = php_cassandra_set_properties;
#if PHP_VERSION_ID >= 50400
  cassandra_set_handlers.get_gc          = php_cassandra_set_gc;
#endif
  cassandra_set_handlers.compare_objects = php_cassandra_set_compare;
  cassandra_set_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
  cassandra_set_ce->create_object = php_cassandra_set_new;
  zend_class_implements(cassandra_set_ce TSRMLS_CC, 2, spl_ce_Countable, zend_ce_iterator);
}
