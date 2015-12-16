#include "php_cassandra.h"
#include "util/types.h"

zend_class_entry *cassandra_boolean_ce = NULL;

void
php_cassandra_boolean_init(INTERNAL_FUNCTION_PARAMETERS)
{
  cassandra_boolean* self;
  zval* value;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &value) == FAILURE) {
    return;
  }

  if (getThis() && instanceof_function(Z_OBJCE_P(getThis()), cassandra_boolean_ce TSRMLS_CC)) {
    self = (cassandra_boolean*) zend_object_store_get_object(getThis() TSRMLS_CC);
  } else {
    object_init_ex(return_value, cassandra_boolean_ce);
    self = (cassandra_boolean*) zend_object_store_get_object(return_value TSRMLS_CC);
  }

  self->value = zval_is_true(value) ? cass_true : cass_false;
}

/* {{{ Cassandra\Boolean::__construct(string) */
PHP_METHOD(Boolean, __construct)
{
  php_cassandra_boolean_init(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
/* }}} */

/* {{{ Cassandra\Boolean::__toString() */
PHP_METHOD(Boolean, __toString)
{
  cassandra_boolean* self = (cassandra_boolean*) zend_object_store_get_object(getThis() TSRMLS_CC);
  if (self->value) {
    RETURN_STRING("true", 1);
  } else {
    RETURN_STRING("false", 1);
  }
}
/* }}} */

/* {{{ Cassandra\Boolean::type() */
PHP_METHOD(Boolean, type)
{
  cassandra_boolean* self = (cassandra_boolean*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_ZVAL(self->type, 1, 0);
}
/* }}} */

/* {{{ Cassandra\Boolean::value() */
PHP_METHOD(Boolean, value)
{
  cassandra_boolean* self = (cassandra_boolean*) zend_object_store_get_object(getThis() TSRMLS_CC);
  if (self->value) {
    RETURN_STRING("true", 1);
  } else {
    RETURN_STRING("false", 1);
  }
}
/* }}} */

ZEND_BEGIN_ARG_INFO_EX(arginfo__construct, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, address)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_none, 0, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_boolean_methods[] = {
  PHP_ME(Boolean, __construct, arginfo__construct, ZEND_ACC_CTOR|ZEND_ACC_PUBLIC)
  PHP_ME(Boolean, __toString, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_boolean_handlers;

static HashTable*
php_cassandra_boolean_gc(zval *object, zval ***table, int *n TSRMLS_DC)
{
  *table = NULL;
  *n = 0;
  return zend_std_get_properties(object TSRMLS_CC);
}

static HashTable*
php_cassandra_boolean_properties(zval *object TSRMLS_DC)
{
  cassandra_boolean* self  = (cassandra_boolean*) zend_object_store_get_object(object TSRMLS_CC);
  HashTable*        props = zend_std_get_properties(object TSRMLS_CC);

  zval* value;

  MAKE_STD_ZVAL(value);

  if (self->value) {
    ZVAL_STRING(value, "true", 1);
  } else {
    ZVAL_STRING(value, "false", 1);
  }

  zend_hash_update(props, "value", sizeof("value"), &value, sizeof(zval), NULL);

  return props;
}

static int
php_cassandra_boolean_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  cassandra_boolean* boolean1 = NULL;
  cassandra_boolean* boolean2 = NULL;

  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  boolean1 = (cassandra_boolean*) zend_object_store_get_object(obj1 TSRMLS_CC);
  boolean2 = (cassandra_boolean*) zend_object_store_get_object(obj2 TSRMLS_CC);

  return boolean1->value < boolean2->value ? -1 : boolean1->value > boolean2->value;
}

static void
php_cassandra_boolean_free(void *object TSRMLS_DC)
{
  cassandra_boolean* self = (cassandra_boolean*) object;

  zval_ptr_dtor(&self->type);
  zend_object_std_dtor(&self->zval TSRMLS_CC);

  efree(self);
}

static zend_object_value
php_cassandra_boolean_new(zend_class_entry* class_type TSRMLS_DC)
{
  zend_object_value retval;
  cassandra_boolean* self;

  self = (cassandra_boolean*) emalloc(sizeof(cassandra_boolean));
  memset(self, 0, sizeof(cassandra_boolean));

  self->type = php_cassandra_type_scalar(CASS_VALUE_TYPE_ASCII TSRMLS_CC);

  zend_object_std_init(&self->zval, class_type TSRMLS_CC);
  object_properties_init(&self->zval, class_type);

  retval.handle   = zend_objects_store_put(self, (zend_objects_store_dtor_t) zend_objects_destroy_object, php_cassandra_boolean_free, NULL TSRMLS_CC);
  retval.handlers = &cassandra_boolean_handlers;

  return retval;
}

void cassandra_define_Boolean(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Boolean", cassandra_boolean_methods);
  cassandra_boolean_ce = zend_register_internal_class(&ce TSRMLS_CC);
  memcpy(&cassandra_boolean_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_boolean_handlers.get_properties  = php_cassandra_boolean_properties;
#if PHP_VERSION_ID >= 50400
  cassandra_boolean_handlers.get_gc          = php_cassandra_boolean_gc;
#endif
  cassandra_boolean_handlers.compare_objects = php_cassandra_boolean_compare;
  cassandra_boolean_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
  cassandra_boolean_ce->create_object = php_cassandra_boolean_new;
}
