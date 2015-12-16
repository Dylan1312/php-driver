#include "php_cassandra.h"
#include "util/math.h"
#include "util/types.h"

#if !defined(HAVE_STDINT_H) && !defined(_MSC_STDINT_H_)
#  define INT32_MAX 2147483647
#  define INT32_MIN (-INT_MAX-1)
#endif

zend_class_entry* cassandra_int_ce = NULL;

static int
to_double(zval* result, cassandra_int* i TSRMLS_DC)
{
  ZVAL_DOUBLE(result, (double) i->value);
  return SUCCESS;
}

static int
to_long(zval* result, cassandra_int* i TSRMLS_DC)
{
  ZVAL_LONG(result, (long) i->value);
  return SUCCESS;
}

static int
to_string(zval* result, cassandra_int* i TSRMLS_DC)
{
  char* string;
  spprintf(&string, 0, "%d", i->value);
  ZVAL_STRING(result, string, 0);
  return SUCCESS;
}

void
php_cassandra_int_init(INTERNAL_FUNCTION_PARAMETERS)
{
  cassandra_int* self;
  zval* value;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &value) == FAILURE) {
    return;
  }

  if (getThis() && instanceof_function(Z_OBJCE_P(getThis()), cassandra_int_ce TSRMLS_CC)) {
    self = (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);
  } else {
    object_init_ex(return_value, cassandra_int_ce);
    self = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);
  }

  if (Z_TYPE_P(value) == IS_LONG) {
    self->value = (cass_int32_t) Z_LVAL_P(value);
  } else if (Z_TYPE_P(value) == IS_DOUBLE) {
    self->value = (cass_int32_t) Z_DVAL_P(value);
  } else if (Z_TYPE_P(value) == IS_STRING) {
    if (!php_cassandra_parse_int(Z_STRVAL_P(value), Z_STRLEN_P(value),
                                    &self->value TSRMLS_CC)) {
      return;
    }
  } else if (Z_TYPE_P(value) == IS_OBJECT &&
             instanceof_function(Z_OBJCE_P(value), cassandra_int_ce TSRMLS_CC)) {
    cassandra_int* integer = (cassandra_int*)
                            zend_object_store_get_object(value TSRMLS_CC);
    self->value = integer->value;
  } else {
    INVALID_ARGUMENT(value, "a long, a double, a numeric string or a " \
                            "Cassandra\\Int");
  }
}

/* {{{ Cassandra\Int::__construct(string) */
PHP_METHOD(Int, __construct)
{
  php_cassandra_int_init(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
/* }}} */

/* {{{ Cassandra\Int::__toString() */
PHP_METHOD(Int, __toString)
{
  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);

  to_string(return_value, self TSRMLS_CC);
}
/* }}} */

/* {{{ Cassandra\Int::type() */
PHP_METHOD(Int, type)
{
  cassandra_int* self = (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_ZVAL(self->type, 1, 0);
}
/* }}} */

/* {{{ Cassandra\Int::value() */
PHP_METHOD(Int, value)
{
  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);

  to_string(return_value, self TSRMLS_CC);
}
/* }}} */

/* {{{ Cassandra\Int::add() */
PHP_METHOD(Int, add)
{
  zval* num;
  cassandra_int* self;
  cassandra_int* integer;
  cassandra_int* result;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_int_ce TSRMLS_CC)) {
    self = (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);
    integer = (cassandra_int*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_int_ce);
    result = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);

    result->value = self->value + integer->value;
  } else {
    INVALID_ARGUMENT(num, "a Cassandra\\Int");
  }
}
/* }}} */

/* {{{ Cassandra\Int::sub() */
PHP_METHOD(Int, sub)
{
  zval* num;
  cassandra_int* result = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_int_ce TSRMLS_CC)) {
    cassandra_int* self =
        (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);
    cassandra_int* integer =
        (cassandra_int*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_int_ce);
    result = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);

    result->value = self->value - integer->value;
  } else {
    INVALID_ARGUMENT(num, "a Cassandra\\Int");
  }
}
/* }}} */

/* {{{ Cassandra\Int::mul() */
PHP_METHOD(Int, mul)
{
  zval* num;
  cassandra_int* result = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_int_ce TSRMLS_CC)) {
    cassandra_int* self =
        (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);
    cassandra_int* integer =
        (cassandra_int*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_int_ce);
    result = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);

    result->value = self->value * integer->value;
  } else {
    INVALID_ARGUMENT(num, "a Cassandra\\Int");
  }
}
/* }}} */

/* {{{ Cassandra\Int::div() */
PHP_METHOD(Int, div)
{
  zval* num;
  cassandra_int* result = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_int_ce TSRMLS_CC)) {
    cassandra_int* self =
        (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);
    cassandra_int* integer =
        (cassandra_int*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_int_ce);
    result = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);

    if (integer->value == 0) {
      zend_throw_exception_ex(cassandra_divide_by_zero_exception_ce, 0 TSRMLS_CC, "Cannot divide by zero");
      return;
    }

    result->value = self->value / integer->value;
  } else {
    INVALID_ARGUMENT(num, "a Cassandra\\Int");
  }
}
/* }}} */

/* {{{ Cassandra\Int::mod() */
PHP_METHOD(Int, mod)
{
  zval* num;
  cassandra_int* result = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_int_ce TSRMLS_CC)) {
    cassandra_int* self =
        (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);
    cassandra_int* integer =
        (cassandra_int*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_int_ce);
    result = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);

    if (integer->value == 0) {
      zend_throw_exception_ex(cassandra_divide_by_zero_exception_ce, 0 TSRMLS_CC, "Cannot modulo by zero");
      return;
    }

    result->value = self->value % integer->value;
  } else {
    INVALID_ARGUMENT(num, "a Cassandra\\Int");
  }
}
/* }}} */

/* {{{ Cassandra\Int::abs() */
PHP_METHOD(Int, abs)
{
  cassandra_int* result = NULL;

  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (self->value == INT32_MIN) {
    zend_throw_exception_ex(cassandra_range_exception_ce, 0 TSRMLS_CC, "Value doesn't exist");
    return;
  }

  object_init_ex(return_value, cassandra_int_ce);
  result = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);
  result->value = self->value < 0 ? -self->value : self->value;
}
/* }}} */

/* {{{ Cassandra\Int::neg() */
PHP_METHOD(Int, neg)
{
  cassandra_int* result = NULL;

  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);

  object_init_ex(return_value, cassandra_int_ce);
  result = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);
  result->value = -self->value;
}
/* }}} */

/* {{{ Cassandra\Int::sqrt() */
PHP_METHOD(Int, sqrt)
{
  cassandra_int* result = NULL;

  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (self->value < 0) {
    zend_throw_exception_ex(cassandra_range_exception_ce, 0 TSRMLS_CC,
                            "Cannot take a square root of a negative number");
  }

  object_init_ex(return_value, cassandra_int_ce);
  result = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);
  result->value = (cass_int32_t) sqrt((long double) self->value);
}
/* }}} */

/* {{{ Cassandra\Int::toInt() */
PHP_METHOD(Int, toInt)
{
  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);

  to_long(return_value, self TSRMLS_CC);
}
/* }}} */

/* {{{ Cassandra\Int::toDouble() */
PHP_METHOD(Int, toDouble)
{
  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(getThis() TSRMLS_CC);

  to_double(return_value, self TSRMLS_CC);
}
/* }}} */

/* {{{ Cassandra\Int::min() */
PHP_METHOD(Int, min)
{
  cassandra_int* integer = NULL;
  object_init_ex(return_value, cassandra_int_ce);
  integer = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);
  integer->value = INT32_MIN;
}
/* }}} */

/* {{{ Cassandra\Int::max() */
PHP_METHOD(Int, max)
{
  cassandra_int* integer = NULL;
  object_init_ex(return_value, cassandra_int_ce);
  integer = (cassandra_int*) zend_object_store_get_object(return_value TSRMLS_CC);
  integer->value = INT32_MAX;
}
/* }}} */

ZEND_BEGIN_ARG_INFO_EX(arginfo__construct, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_none, 0, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_num, 0, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, num)
ZEND_END_ARG_INFO()

static zend_function_entry cassandra_int_methods[] = {
  PHP_ME(Int, __construct, arginfo__construct, ZEND_ACC_CTOR|ZEND_ACC_PUBLIC)
  PHP_ME(Int, __toString, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Int, value, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Int, add, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Int, sub, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Int, mul, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Int, div, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Int, mod, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Int, abs, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Int, neg, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Int, sqrt, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Int, toInt, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Int, toDouble, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Int, min, arginfo_none, ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
  PHP_ME(Int, max, arginfo_none, ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_int_handlers;

static HashTable*
php_cassandra_int_gc(zval *object, zval ***table, int *n TSRMLS_DC)
{
  *table = NULL;
  *n = 0;
  return zend_std_get_properties(object TSRMLS_CC);
}

static HashTable*
php_cassandra_int_properties(zval *object TSRMLS_DC)
{
  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(object TSRMLS_CC);
  HashTable*        props  = zend_std_get_properties(object TSRMLS_CC);

  zval* value;
  char* string;
  int string_len;

#ifdef WIN32
  string_len = spprintf(&string, 0, "%I64d", (long long int) self->value);
#else
  string_len = spprintf(&string, 0, "%lld", (long long int) self->value);
#endif

  MAKE_STD_ZVAL(value);
  ZVAL_STRINGL(value, string, string_len, 0);

  zend_hash_update(props, "value", sizeof("value"), &value, sizeof(zval), NULL);

  return props;
}

static int
php_cassandra_int_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  cassandra_int* int1 = NULL;
  cassandra_int* int2 = NULL;

  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  int1 = (cassandra_int*) zend_object_store_get_object(obj1 TSRMLS_CC);
  int2 = (cassandra_int*) zend_object_store_get_object(obj2 TSRMLS_CC);

  if (int1->value == int2->value)
    return 0;
  else if (int1->value < int2->value)
    return -1;
  else
    return 1;
}

static int
php_cassandra_int_cast(zval* object, zval* retval, int type TSRMLS_DC)
{
  cassandra_int* self =
      (cassandra_int*) zend_object_store_get_object(object TSRMLS_CC);

  switch (type) {
  case IS_LONG:
      return to_long(retval, self TSRMLS_CC);
  case IS_DOUBLE:
      return to_double(retval, self TSRMLS_CC);
  case IS_STRING:
      return to_string(retval, self TSRMLS_CC);
  default:
     return FAILURE;
  }

  return SUCCESS;
}

static void
php_cassandra_int_free(void *object TSRMLS_DC)
{
  cassandra_int* self = (cassandra_int*) object;

  zval_ptr_dtor(&self->type);
  zend_object_std_dtor(&self->zval TSRMLS_CC);

  efree(self);
}

static zend_object_value
php_cassandra_int_new(zend_class_entry* class_type TSRMLS_DC)
{
  zend_object_value retval;
  cassandra_int *self;

  self = (cassandra_int*) emalloc(sizeof(cassandra_int));
  memset(self, 0, sizeof(cassandra_int));

  self->type = php_cassandra_type_scalar(CASS_VALUE_TYPE_BIGINT TSRMLS_CC);

  zend_object_std_init(&self->zval, class_type TSRMLS_CC);
  object_properties_init(&self->zval, class_type);

  retval.handle   = zend_objects_store_put(self, (zend_objects_store_dtor_t) zend_objects_destroy_object, php_cassandra_int_free, NULL TSRMLS_CC);
  retval.handlers = &cassandra_int_handlers;

  return retval;
}

void cassandra_define_Int(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Int", cassandra_int_methods);
  cassandra_int_ce = zend_register_internal_class(&ce TSRMLS_CC);
  zend_class_implements(cassandra_int_ce TSRMLS_CC, 1, cassandra_numeric_ce);
  cassandra_int_ce->ce_flags     |= ZEND_ACC_FINAL_CLASS;
  cassandra_int_ce->create_object = php_cassandra_int_new;

  memcpy(&cassandra_int_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_int_handlers.get_properties  = php_cassandra_int_properties;
#if PHP_VERSION_ID >= 50400
  cassandra_int_handlers.get_gc          = php_cassandra_int_gc;
#endif
  cassandra_int_handlers.compare_objects = php_cassandra_int_compare;
  cassandra_int_handlers.cast_object     = php_cassandra_int_cast;
}
