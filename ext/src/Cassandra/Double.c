#include "php_cassandra.h"
#include "util/math.h"
#include "util/types.h"
#include <float.h>

zend_class_entry* cassandra_double_ce = NULL;

static int
to_string(zval* result, cassandra_double* dbl TSRMLS_DC)
{
  char* string;
  spprintf(&string, 0, "%.*F", (int) EG(precision), dbl->value);
  ZVAL_STRING(result, string, 0);
  return SUCCESS;
}

void
php_cassandra_double_init(INTERNAL_FUNCTION_PARAMETERS)
{
  cassandra_double* self;
  zval* value;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &value) == FAILURE) {
    return;
  }

  if (getThis() && instanceof_function(Z_OBJCE_P(getThis()), cassandra_double_ce TSRMLS_CC)) {
    self = (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
  } else {
    object_init_ex(return_value, cassandra_double_ce);
    self = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);
  }

  if (Z_TYPE_P(value) == IS_LONG) {
    self->value = (cass_double_t) Z_LVAL_P(value);
  } else if (Z_TYPE_P(value) == IS_DOUBLE) {
    self->value = (cass_double_t) Z_DVAL_P(value);
  } else if (Z_TYPE_P(value) == IS_STRING) {
    if (!php_cassandra_parse_double(Z_STRVAL_P(value), Z_STRLEN_P(value),
                                   &self->value TSRMLS_CC)) {
      return;
    }
  } else if (Z_TYPE_P(value) == IS_OBJECT &&
             instanceof_function(Z_OBJCE_P(value), cassandra_double_ce TSRMLS_CC)) {
    cassandra_double* dbl =
        (cassandra_double*) zend_object_store_get_object(value TSRMLS_CC);
    self->value = dbl->value;
  } else {
    INVALID_ARGUMENT(value, "a long, double, numeric string or a " \
                            "Cassandra\\Double instance");
  }
}

/* {{{ Cassandra\Double::__construct(string) */
PHP_METHOD(Double, __construct)
{
  php_cassandra_double_init(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
/* }}} */

/* {{{ Cassandra\Double::__toString() */
PHP_METHOD(Double, __toString)
{
  cassandra_double* self = (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);

  to_string(return_value, self TSRMLS_CC);
}
/* }}} */

/* {{{ Cassandra\Double::type() */
PHP_METHOD(Double, type)
{
  cassandra_double* self = (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_ZVAL(self->type, 1, 0);
}
/* }}} */

/* {{{ Cassandra\Double::value() */
PHP_METHOD(Double, value)
{
  cassandra_double* self = (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_DOUBLE((double) self->value);
}
/* }}} */

/* {{{ Cassandra\Double::isInfinite() */
PHP_METHOD(Double, isInfinite)
{
  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_BOOL(zend_isinf(self->value));
}
/* }}} */

/* {{{ Cassandra\Double::isFinite() */
PHP_METHOD(Double, isFinite)
{
  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_BOOL(zend_finite(self->value));
}
/* }}} */

/* {{{ Cassandra\Double::isNaN() */
PHP_METHOD(Double, isNaN)
{
  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
  RETURN_BOOL(zend_isnan(self->value));
}
/* }}} */

/* {{{ Cassandra\Double::add() */
PHP_METHOD(Double, add)
{
  zval* num;
  cassandra_double* result = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_double_ce TSRMLS_CC)) {
    cassandra_double* self =
        (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
    cassandra_double* dbl =
        (cassandra_double*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_double_ce);
    result = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);

    result->value = self->value + dbl->value;
  } else {
    INVALID_ARGUMENT(num, "an instance of Cassandra\\Double");
  }
}
/* }}} */

/* {{{ Cassandra\Double::sub() */
PHP_METHOD(Double, sub)
{
  zval* num;
  cassandra_double* result = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_double_ce TSRMLS_CC)) {
    cassandra_double* self =
        (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
    cassandra_double* dbl =
        (cassandra_double*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_double_ce);
    result = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);

    result->value = self->value - dbl->value;
  } else {
    INVALID_ARGUMENT(num, "an instance of Cassandra\\Double");
  }
}
/* }}} */

/* {{{ Cassandra\Double::mul() */
PHP_METHOD(Double, mul)
{
  zval* num;
  cassandra_double* result = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_double_ce TSRMLS_CC)) {
    cassandra_double* self =
        (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
    cassandra_double* dbl =
        (cassandra_double*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_double_ce);
    result = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);

    result->value = self->value * dbl->value;
  } else {
    INVALID_ARGUMENT(num, "an instance of Cassandra\\Double");
  }
}
/* }}} */

/* {{{ Cassandra\Double::div() */
PHP_METHOD(Double, div)
{
  zval* num;
  cassandra_double* result = NULL;

  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z", &num) == FAILURE) {
    return;
  }

  if (Z_TYPE_P(num) == IS_OBJECT &&
      instanceof_function(Z_OBJCE_P(num), cassandra_double_ce TSRMLS_CC)) {
    cassandra_double* self =
        (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
    cassandra_double* dbl =
        (cassandra_double*) zend_object_store_get_object(num TSRMLS_CC);

    object_init_ex(return_value, cassandra_double_ce);
    result = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);

    if (dbl->value == 0) {
      zend_throw_exception_ex(cassandra_divide_by_zero_exception_ce, 0 TSRMLS_CC, "Cannot divide by zero");
      return;
    }

    result->value = self->value / dbl->value;
  } else {
    INVALID_ARGUMENT(num, "an instance of Cassandra\\Double");
  }
}
/* }}} */

/* {{{ Cassandra\Double::mod() */
PHP_METHOD(Double, mod)
{
  /* TODO: We could use fmod() here, but maybe we should add a remainder function
   * for floating point types.
   */
  zend_throw_exception_ex(cassandra_runtime_exception_ce, 0 TSRMLS_CC, "Not implemented");
}

/* {{{ Cassandra\Double::abs() */
PHP_METHOD(Double, abs)
{
  cassandra_double* result = NULL;

  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
  object_init_ex(return_value, cassandra_double_ce);
  result = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);
  result->value = fabs(self->value);
}
/* }}} */

/* {{{ Cassandra\Double::neg() */
PHP_METHOD(Double, neg)
{
  cassandra_double* result = NULL;

  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);
  object_init_ex(return_value, cassandra_double_ce);
  result = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);
  result->value = -self->value;
}
/* }}} */

/* {{{ Cassandra\Double::sqrt() */
PHP_METHOD(Double, sqrt)
{
  cassandra_double* result = NULL;

  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);

  if (self->value < 0) {
    zend_throw_exception_ex(cassandra_range_exception_ce, 0 TSRMLS_CC,
                            "Cannot take a square root of a negative number");
  }

  object_init_ex(return_value, cassandra_double_ce);
  result = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);
  result->value = sqrt(self->value);
}
/* }}} */

/* {{{ Cassandra\Double::toInt() */
PHP_METHOD(Double, toInt)
{
  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);

  RETURN_LONG((long) self->value);
}
/* }}} */

/* {{{ Cassandra\Double::toDouble() */
PHP_METHOD(Double, toDouble)
{
  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(getThis() TSRMLS_CC);

  RETURN_DOUBLE((double) self->value);
}
/* }}} */

/* {{{ Cassandra\Double::min() */
PHP_METHOD(Double, min)
{
  cassandra_double* dbl = NULL;
  object_init_ex(return_value, cassandra_double_ce);
  dbl = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);
  dbl->value = DBL_MIN;
}
/* }}} */

/* {{{ Cassandra\Double::max() */
PHP_METHOD(Double, max)
{
  cassandra_double* dbl = NULL;
  object_init_ex(return_value, cassandra_double_ce);
  dbl = (cassandra_double*) zend_object_store_get_object(return_value TSRMLS_CC);
  dbl->value = DBL_MAX;
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

static zend_function_entry cassandra_double_methods[] = {
  PHP_ME(Double, __construct, arginfo__construct, ZEND_ACC_CTOR|ZEND_ACC_PUBLIC)
  PHP_ME(Double, __toString, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, value, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, isInfinite, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, isFinite, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, isNaN, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, add, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Double, sub, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Double, mul, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Double, div, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Double, mod, arginfo_num, ZEND_ACC_PUBLIC)
  PHP_ME(Double, abs, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, neg, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, sqrt, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, toInt, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, toDouble, arginfo_none, ZEND_ACC_PUBLIC)
  PHP_ME(Double, min, arginfo_none, ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
  PHP_ME(Double, max, arginfo_none, ZEND_ACC_PUBLIC|ZEND_ACC_STATIC)
  PHP_FE_END
};

static zend_object_handlers cassandra_double_handlers;

static HashTable*
php_cassandra_double_gc(zval *object, zval ***table, int *n TSRMLS_DC)
{
  *table = NULL;
  *n = 0;
  return zend_std_get_properties(object TSRMLS_CC);
}

static HashTable*
php_cassandra_double_properties(zval *object TSRMLS_DC)
{
  cassandra_double* self = (cassandra_double*) zend_object_store_get_object(object TSRMLS_CC);
  HashTable*       props  = zend_std_get_properties(object TSRMLS_CC);

  zval* value;
  char* string;
  int string_len;

  string_len = spprintf(&string, 0, "%.*F", (int) EG(precision), self->value);

  MAKE_STD_ZVAL(value);
  ZVAL_STRINGL(value, string, string_len, 0);

  zend_hash_update(props, "value", sizeof("value"), &value, sizeof(zval), NULL);

  return props;
}

static int
php_cassandra_double_compare(zval *obj1, zval *obj2 TSRMLS_DC)
{
  cassandra_double* dbl1 = NULL;
  cassandra_double* dbl2 = NULL;

  if (Z_OBJCE_P(obj1) != Z_OBJCE_P(obj2))
    return 1; /* different classes */

  dbl1 = (cassandra_double*) zend_object_store_get_object(obj1 TSRMLS_CC);
  dbl2 = (cassandra_double*) zend_object_store_get_object(obj2 TSRMLS_CC);

  if (dbl1->value == dbl2->value)
    return 0;
  else if (dbl1->value < dbl2->value)
    return -1;
  else
    return 1;
}

static int
php_cassandra_double_cast(zval* object, zval* retval, int type TSRMLS_DC)
{
  cassandra_double* self =
      (cassandra_double*) zend_object_store_get_object(object TSRMLS_CC);

  switch (type) {
  case IS_LONG:
      ZVAL_LONG(retval, (long) self->value);
      return SUCCESS;
  case IS_DOUBLE:
      ZVAL_DOUBLE(retval, (double) self->value);
      return SUCCESS;
  case IS_STRING:
      return to_string(retval, self TSRMLS_CC);
  default:
     return FAILURE;
  }

  return SUCCESS;
}

static void
php_cassandra_double_free(void *object TSRMLS_DC)
{
  cassandra_double* self = (cassandra_double*) object;

  zval_ptr_dtor(&self->type);
  zend_object_std_dtor(&self->zval TSRMLS_CC);

  efree(self);
}

static zend_object_value
php_cassandra_double_new(zend_class_entry* class_type TSRMLS_DC)
{
  zend_object_value retval;
  cassandra_double *self;

  self = (cassandra_double*) emalloc(sizeof(cassandra_double));
  memset(self, 0, sizeof(cassandra_double));

  self->type = php_cassandra_type_scalar(CASS_VALUE_TYPE_FLOAT TSRMLS_CC);

  zend_object_std_init(&self->zval, class_type TSRMLS_CC);
  object_properties_init(&self->zval, class_type);

  retval.handle   = zend_objects_store_put(self, (zend_objects_store_dtor_t) zend_objects_destroy_object, php_cassandra_double_free, NULL TSRMLS_CC);
  retval.handlers = &cassandra_double_handlers;

  return retval;
}

void cassandra_define_Double(TSRMLS_D)
{
  zend_class_entry ce;

  INIT_CLASS_ENTRY(ce, "Cassandra\\Double", cassandra_double_methods);
  cassandra_double_ce = zend_register_internal_class(&ce TSRMLS_CC);
  zend_class_implements(cassandra_double_ce TSRMLS_CC, 1, cassandra_numeric_ce);
  cassandra_double_ce->ce_flags     |= ZEND_ACC_FINAL_CLASS;
  cassandra_double_ce->create_object = php_cassandra_double_new;

  memcpy(&cassandra_double_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
  cassandra_double_handlers.get_properties  = php_cassandra_double_properties;
#if PHP_VERSION_ID >= 50400
  cassandra_double_handlers.get_gc          = php_cassandra_double_gc;
#endif
  cassandra_double_handlers.compare_objects = php_cassandra_double_compare;
  cassandra_double_handlers.cast_object     = php_cassandra_double_cast;
}
