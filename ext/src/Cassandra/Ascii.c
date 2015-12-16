#include "php_cassandra.h"
#include "util/types.h"

zend_class_entry *cassandra_ascii_ce = NULL;

void
php_cassandra_ascii_init(INTERNAL_FUNCTION_PARAMETERS)
{
  cassandra_string* self;
  char* string;
  int string_len;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &string, &string_len) == FAILURE) {
    return;
  }

  if (getThis() && instanceof_function(Z_OBJCE_P(getThis()), cassandra_ascii_ce TSRMLS_CC)) {
    self = (cassandra_string*) zend_object_store_get_object(getThis() TSRMLS_CC);
  } else {
    object_init_ex(return_value, cassandra_ascii_ce);
    self = (cassandra_string*) zend_object_store_get_object(return_value TSRMLS_CC);
  }

  self->data = estrdup(string);
  self->len = string_len;
}

/* {{{ Cassandra\Ascii::__construct(string) */
PHP_METHOD(Ascii, __construct)
{
  php_cassandra_ascii_init(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
/* }}} */

/* {{{ Cassandra\Ascii::__toString() */
PHP_METHOD(Ascii, __toString)
{
  cassandra_string* self = (cassandra_string*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_STRINGL(self->data, self->len, 1);
}
/* }}} */

/* {{{ Cassandra\Ascii::type() */
PHP_METHOD(Ascii, type)
{
  cassandra_string* self = (cassandra_string*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_ZVAL(self->type, 1, 0);
}
/* }}} */

/* {{{ Cassandra\Ascii::value() */
PHP_METHOD(Ascii, value)
{
  cassandra_string* self = (cassandra_string*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_STRINGL(self->data, self->len, 1);
}
/* }}} */

ZEND_BEGIN_ARG_INFO_EX(arginfo__construct, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, address)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_none, 0, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_ascii_methods[] = {
  PHP_ME(Ascii, __construct, arginfo__construct, ZEND_ACC_CTOR|ZEND_ACC_PUBLIC)
  PHP_ME(Ascii, __toString, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_ascii_handlers;

static HashTable*
php_cassandra_ascii_gc(zval *object, zval ***table, int *n TSRMLS_DC)
{
  *table = NULL;
  *n = 0;
  return zend_std_get_properties(object TSRMLS_CC);
}

static HashTable*
php_cassandra_ascii_properties(zval *object TSRMLS_DC)
{
  cassandra_string* self  = (cassandra_string*) zend_object_store_get_object(object TSRMLS_CC);
  HashTable*        props = zend_std_get_properties(object TSRMLS_CC);

  zval* value;

  MAKE_STD_ZVAL(value);
  ZVAL_STRINGL(value, self->data, self->len, 1);

  zend_hash_update(props, "value", sizeof("value"), &value, sizeof(zval), NULL);

  return props;
}

static int
php_cassandra_ascii_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  cassandra_string* ascii1 = NULL;
  cassandra_string* ascii2 = NULL;

  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  ascii1 = (cassandra_string*) zend_object_store_get_object(obj1 TSRMLS_CC);
  ascii2 = (cassandra_string*) zend_object_store_get_object(obj2 TSRMLS_CC);

  if (ascii1->len != ascii2->len) {
    return ascii1->len < ascii2->len ? -1 : 1;
  }

  return strncmp(ascii1->data, ascii2->data, ascii1->len);
}

static void
php_cassandra_ascii_free(void *object TSRMLS_DC)
{
  cassandra_string* self = (cassandra_string*) object;

  if (self->data) efree(self->data);

  zval_ptr_dtor(&self->type);
  zend_object_std_dtor(&self->zval TSRMLS_CC);

  efree(self);
}

static zend_object_value
php_cassandra_ascii_new(zend_class_entry* class_type TSRMLS_DC)
{
  zend_object_value retval;
  cassandra_string* self;

  self = (cassandra_string*) emalloc(sizeof(cassandra_string));
  memset(self, 0, sizeof(cassandra_string));

  self->type = php_cassandra_type_scalar(CASS_VALUE_TYPE_ASCII TSRMLS_CC);

  zend_object_std_init(&self->zval, class_type TSRMLS_CC);
  object_properties_init(&self->zval, class_type);

  retval.handle   = zend_objects_store_put(self, (zend_objects_store_dtor_t) zend_objects_destroy_object, php_cassandra_ascii_free, NULL TSRMLS_CC);
  retval.handlers = &cassandra_ascii_handlers;

  return retval;
}

void cassandra_define_Ascii(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Ascii", cassandra_ascii_methods);
  cassandra_ascii_ce = zend_register_internal_class(&ce TSRMLS_CC);
  memcpy(&cassandra_ascii_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_ascii_handlers.get_properties  = php_cassandra_ascii_properties;
#if PHP_VERSION_ID >= 50400
  cassandra_ascii_handlers.get_gc          = php_cassandra_ascii_gc;
#endif
  cassandra_ascii_handlers.compare_objects = php_cassandra_ascii_compare;
  cassandra_ascii_ce->ce_flags |= ZEND_ACC_FINAL_CLASS;
  cassandra_ascii_ce->create_object = php_cassandra_ascii_new;
}
