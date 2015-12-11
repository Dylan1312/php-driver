#include "php_cassandra.h"
#include <stdlib.h>
#include "util/bytes.h"
#include "util/collections.h"
#include "util/hash.h"
#include "util/math.h"
#include "util/types.h"

#define EXPECTING_VALUE(expected) \
{ \
  throw_invalid_argument(object, "argument", expected TSRMLS_CC); \
  return 0; \
}

#define INSTANCE_OF(cls) \
  (Z_TYPE_P(object) == IS_OBJECT && instanceof_function(Z_OBJCE_P(object), cls TSRMLS_CC))

#define CHECK_ERROR(rc) ASSERT_SUCCESS_BLOCK(rc, result = 0;)

static inline unsigned
combine_hash(unsigned seed, unsigned  hashv) {
  return seed ^ (hashv + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

static inline unsigned
string_hash(cassandra_string* value) {
  return zend_inline_hash_func(value->data, value->size);
}

static inline unsigned
blob_hash(cassandra_blob* value) {
  return zend_inline_hash_func((const char*)value->data, value->size);
}

static inline unsigned
bigint_hash(cass_int64_t value) {
  return (unsigned)(value ^ (value >> 32));
}

static inline unsigned
float_hash(cass_float_t value) {
  // TODO(mpenick):
  return 0;
}

static inline unsigned
double_hash(cass_float_t value) {
  // TODO(mpenick):
  return 0;
}

static inline unsigned
decimal_hash(cassandra_decimal* value) {
  // TODO(mpenick):
  return 0;
}

static inline unsigned
uuid_hash(cassandra_uuid* value) {
  return combine_hash(bigint_hash(value->uuid.time_and_version),
                      bigint_hash(value->uuid.clock_seq_and_node));
}

static inline unsigned
varint_hash(cassandra_varint* value) {
  // TODO(mpenick):
  return 0;
}

static inline unsigned
inet_hash(cassandra_inet* value) {
  return zend_inline_hash_func((const char*)value->inet.address,
                               value->inet.address_length);
}

static inline unsigned
collection_hash(cassandra_collection* value TSRMLS_DC) {
  // TODO(mpenick):
  return 0;
}

static inline unsigned
map_hash(cassandra_map* value TSRMLS_DC) {
  cassandra_map_entry* curr, * temp;
  unsigned hashv = 0x9e3779b9;
  if (!value->dirty) return value->hashv;
  HASH_ITER(hh, value->entries, curr, temp) {
    hashv = combine_hash(hashv, php_cassandra_value_hash(curr->key TSRMLS_CC));
    hashv = combine_hash(hashv, php_cassandra_value_hash(curr->value TSRMLS_CC));
  }
  value->hashv = hashv;
  value->dirty = 0;
  return hashv;
}

static inline unsigned
set_hash(cassandra_set* value TSRMLS_DC) {
  cassandra_set_entry* curr, * temp;
  unsigned hashv = 0x9e3779b9;
  if (!value->dirty) return value->hashv;
  HASH_ITER(hh, value->entries, curr, temp) {
    hashv = combine_hash(hashv, php_cassandra_value_hash(curr->element TSRMLS_CC));
  }
  value->hashv = hashv;
  value->dirty = 0;
  return hashv;
}

unsigned
php_cassandra_value_hash(const zval* zvalue TSRMLS_DC) {
  const cassandra_value* value = (const cassandra_value*) zend_object_store_get_object(zvalue TSRMLS_CC);

  switch(value->type) {
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_TEXT:
      return string_hash((cassandra_string*)value);

    case CASS_VALUE_TYPE_BIGINT:
      return bigint_hash(((cassandra_bigint*)value)->value);

    case CASS_VALUE_TYPE_BLOB:
      return blob_hash((cassandra_blob*)value);

    case CASS_VALUE_TYPE_BOOLEAN:
      return (unsigned)((cassandra_boolean*)value)->value;

    case CASS_VALUE_TYPE_COUNTER:
      return bigint_hash(((cassandra_counter*)value)->counter);

    case CASS_VALUE_TYPE_DECIMAL:
      return decimal_hash((cassandra_decimal*)value);

    case CASS_VALUE_TYPE_DOUBLE:
      return double_hash(((cassandra_double*)value)->value);

    case CASS_VALUE_TYPE_FLOAT:
      return float_hash(((cassandra_float*)value)->value);

    case CASS_VALUE_TYPE_INT:
      return ((cassandra_int*)value)->value;

    case CASS_VALUE_TYPE_TIMESTAMP:
      return bigint_hash(((cassandra_timestamp*)value)->timestamp);

    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_TIMEUUID:
      return uuid_hash((cassandra_uuid*)value);

    case CASS_VALUE_TYPE_VARINT:
      return varint_hash((cassandra_varint*)value);

    case CASS_VALUE_TYPE_INET:
      return inet_hash((cassandra_inet*)value);

    case CASS_VALUE_TYPE_LIST:
      return collection_hash((cassandra_collection*)value TSRMLS_CC);

    case CASS_VALUE_TYPE_MAP:
      return map_hash((cassandra_map*)value TSRMLS_CC);

    case CASS_VALUE_TYPE_SET:
      return set_hash((cassandra_set*)value TSRMLS_CC);

    default:
      break;
  }

  return 0;
}

static inline int
string_compare(cassandra_string* value1, cassandra_string* value2) {
  if (value1->size != value2->size)
    return value1->size < value2->size ? -1 : 1;
  return memcmp(value1->data, value1->data, value1->size);
}

static inline int
blob_compare(cassandra_blob* value1, cassandra_blob* value2) {
  if (value1->size != value2->size)
    return value1->size < value2->size ? -1 : 1;
  return memcmp(value1->data, value1->data, value1->size);
}

static inline int
double_compare(cassandra_double* value1, cassandra_double* value2) {
  // TODO(mpenick):
  return 0;
}

static inline int
float_compare(cassandra_float* value1, cassandra_float* value2) {
  // TODO(mpenick):
  return 0;
}

static inline int
decimal_compare(cassandra_decimal* value1, cassandra_decimal* value2) {
  // TODO(mpenick):
  return 0;
}

static inline int
uuid_compare_(cassandra_uuid* value1, cassandra_uuid* value2) {
  if (value1->uuid.time_and_version != value2->uuid.time_and_version)
    return value1->uuid.time_and_version < value2->uuid.time_and_version ? -1 : 1;
  if (value1->uuid.clock_seq_and_node != value2->uuid.clock_seq_and_node)
    return value1->uuid.clock_seq_and_node < value2->uuid.clock_seq_and_node ? -1 : 1;
  return 0;
}

static inline int
varint_compare(cassandra_varint* value1, cassandra_varint* value2) {
  // TODO(mpenick):
  return 0;
}

static inline int
inet_compare(cassandra_inet* value1, cassandra_inet* value2) {
  // TODO(mpenick):
  return 0;
}

static inline int
collection_compare(cassandra_collection* value1, cassandra_collection* value2 TSRMLS_DC) {
  // TODO(mpenick):
  return 0;
}

static inline int
map_compare(cassandra_map* value1, cassandra_map* value2 TSRMLS_DC) {
  // TODO(mpenick):
  return 0;
}

static inline int
set_compare(cassandra_set* value1, cassandra_set* value2 TSRMLS_DC) {
  // TODO(mpenick):
  return 0;
}

#define COMPARE(a, b) ((a) < (b) ? -1 : (a) > (b))

int
php_cassandra_value_compare(const zval* zvalue1, const zval* zvalue2 TSRMLS_DC) {
  const cassandra_value* a;
  const cassandra_value* b;

  if (zvalue1 == zvalue2) return 0;

  a = (const cassandra_value*) zend_object_store_get_object(zvalue1 TSRMLS_CC);
  b = (const cassandra_value*) zend_object_store_get_object(zvalue2 TSRMLS_CC);

  if (a->type != b->type)  return a->type < b->type ? -1 : 1;

  switch(a->type) {
    case CASS_VALUE_TYPE_ASCII:
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_TEXT:
      return string_compare((cassandra_string*)a, (cassandra_string*)b);

    case CASS_VALUE_TYPE_BIGINT:
      return COMPARE(((cassandra_bigint*)a)->value, ((cassandra_bigint*)b)->value);

    case CASS_VALUE_TYPE_BLOB:
      return blob_compare((cassandra_blob*)a, (cassandra_blob*)b);

    case CASS_VALUE_TYPE_BOOLEAN:
      return COMPARE(((cassandra_boolean*)a)->value, ((cassandra_boolean*)b)->value);

    case CASS_VALUE_TYPE_COUNTER:
      return COMPARE(((cassandra_counter*)a)->counter, ((cassandra_counter*)b)->counter);

    case CASS_VALUE_TYPE_DECIMAL:
      return decimal_compare((cassandra_decimal*)a, (cassandra_decimal*)b);

    case CASS_VALUE_TYPE_DOUBLE:
      return double_compare((cassandra_double*)a, (cassandra_double*)b);

    case CASS_VALUE_TYPE_FLOAT:
      return float_compare((cassandra_float*)a, (cassandra_float*)b);

    case CASS_VALUE_TYPE_INT:
      return COMPARE(((cassandra_int*)a)->value, ((cassandra_int*)b)->value);

    case CASS_VALUE_TYPE_TIMESTAMP:
      return COMPARE(((cassandra_timestamp*)a)->timestamp, ((cassandra_timestamp*)b)->timestamp);

    case CASS_VALUE_TYPE_UUID:
    case CASS_VALUE_TYPE_TIMEUUID:
      return uuid_compare_((cassandra_uuid*)a, (cassandra_uuid*)b);

    case CASS_VALUE_TYPE_VARINT:
      return varint_compare((cassandra_varint*)a, (cassandra_varint*)b);

    case CASS_VALUE_TYPE_INET:
      return inet_compare((cassandra_inet*)a, (cassandra_inet*)b);

    case CASS_VALUE_TYPE_LIST:
      return collection_compare((cassandra_collection*)a, (cassandra_collection*)b TSRMLS_CC);

    case CASS_VALUE_TYPE_MAP:
      return map_compare((cassandra_map*)a, (cassandra_map*)b TSRMLS_CC);

    case CASS_VALUE_TYPE_SET:
      return set_compare((cassandra_set*)a, (cassandra_set*)b TSRMLS_CC);

    default:
      break;
  }

  return 0;
}

#undef COMPARE

int
php_cassandra_validate_object(zval* object, zval* ztype, zval** output TSRMLS_DC)
{
  if (Z_TYPE_P(object) == IS_NULL)
    return 1;

  cassandra_type* type = (cassandra_type*) zend_object_store_get_object(ztype TSRMLS_CC);

  switch (type->type) {
  case CASS_VALUE_TYPE_VARCHAR:
  case CASS_VALUE_TYPE_TEXT:
    if (Z_TYPE_P(object) == IS_STRING) {
      // TODO(mpenick): Box type
    } else if(INSTANCE_OF(cassandra_varchar_ce)) {
      if (output) *output = object;
    } else {
      EXPECTING_VALUE("a string");
    }

    return 1;
  case CASS_VALUE_TYPE_ASCII:
    if (Z_TYPE_P(object) == IS_STRING) {
      // TODO(mpenick): Box type
    } else if(INSTANCE_OF(cassandra_ascii_ce)) {
      if (output) *output = object;
    } else {
      EXPECTING_VALUE("a string");
    }

    return 1;

  case CASS_VALUE_TYPE_DOUBLE:
    if (Z_TYPE_P(object) == IS_DOUBLE) {
      // TODO(mpenick): Box type
    } else if(INSTANCE_OF(cassandra_double_ce)) {
      if (output) *output = object;
    } else {
      EXPECTING_VALUE("a float");
    }

    return 1;
  case CASS_VALUE_TYPE_INT:
    if (Z_TYPE_P(object) == IS_LONG) {
      // TODO(mpenick): Box type
    } else if (INSTANCE_OF(cassandra_int_ce)) {
      if (output) *output = object;
    } else {
      EXPECTING_VALUE("an int");
    }

    return 1;
  case CASS_VALUE_TYPE_BOOLEAN:
    if (Z_TYPE_P(object) != IS_BOOL) {
      // TODO(mpenick): Box type
    } else if(INSTANCE_OF(cassandra_boolean_ce)) {
      if (output) *output = object;
    } else {
      EXPECTING_VALUE("a boolean");
    }

    return 1;
  case CASS_VALUE_TYPE_FLOAT:
    if (!INSTANCE_OF(cassandra_float_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Float");
    }

    if (output) *output = object;
    return 1;
  case CASS_VALUE_TYPE_COUNTER:
  case CASS_VALUE_TYPE_BIGINT:
    if (!INSTANCE_OF(cassandra_bigint_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Bigint");
    }

    if (output) *output = object;
    return 1;
  case CASS_VALUE_TYPE_BLOB:
    if (!INSTANCE_OF(cassandra_blob_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Blob");
    }

    if (output) *output = object;
    return 1;
  case CASS_VALUE_TYPE_DECIMAL:
    if (!INSTANCE_OF(cassandra_decimal_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Decimal");
    }

    if (output) *output = object;
    return 1;
  case CASS_VALUE_TYPE_TIMESTAMP:
    if (!INSTANCE_OF(cassandra_timestamp_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Timestamp");
    }

    if (output) *output = object;
    return 1;
  case CASS_VALUE_TYPE_UUID:
    if (!INSTANCE_OF(cassandra_uuid_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Uuid");
    }

    if (output) *output = object;
    return 1;
  case CASS_VALUE_TYPE_VARINT:
    if (!INSTANCE_OF(cassandra_varint_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Varint");
    }

    if (output) *output = object;
    return 1;
  case CASS_VALUE_TYPE_TIMEUUID:
    if (!INSTANCE_OF(cassandra_timeuuid_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Timeuuid");
    }

    if (output) *output = object;
    return 1;
  case CASS_VALUE_TYPE_INET:
    if (!INSTANCE_OF(cassandra_inet_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Inet");
    }

    return 1;
 case CASS_VALUE_TYPE_MAP:
    if (!INSTANCE_OF(cassandra_map_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Map");
    } else {
      cassandra_map* map = (cassandra_map*) zend_object_store_get_object(object TSRMLS_CC);
      cassandra_type* map_type = (cassandra_type*) zend_object_store_get_object(map->ztype TSRMLS_CC);
      if (!php_cassandra_type_equals(map_type, type TSRMLS_CC)) {
        return 0;
      }
    }

    if (output) *output = object;
    return 1;
 case CASS_VALUE_TYPE_SET:
    if (!INSTANCE_OF(cassandra_set_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Set");
    } else {
      cassandra_set* set = (cassandra_set*) zend_object_store_get_object(object TSRMLS_CC);
      cassandra_type* set_type = (cassandra_type*) zend_object_store_get_object(set->ztype TSRMLS_CC);
      if (!php_cassandra_type_equals(set_type, type TSRMLS_CC)) {
        return 0;
      }
    }

    if (output) *output = object;
    return 1;
 case CASS_VALUE_TYPE_LIST:
    if (!INSTANCE_OF(cassandra_collection_ce)) {
      EXPECTING_VALUE("an instance of Cassandra\\Collection");
    } else {
      cassandra_collection* collection = (cassandra_collection*) zend_object_store_get_object(object TSRMLS_CC);
      cassandra_type* collection_type = (cassandra_type*) zend_object_store_get_object(collection->ztype TSRMLS_CC);
      if (!php_cassandra_type_equals(collection_type, type TSRMLS_CC)) {
        return 0;
      }
    }

    if (output) *output = object;
    return 1;

    return 1;

  default:
    EXPECTING_VALUE("a simple Cassandra value");

    return 0;
  }
}

int
php_cassandra_value_type(char* type, CassValueType* value_type TSRMLS_DC)
{
  if (strcmp("ascii", type) == 0) {
    *value_type = CASS_VALUE_TYPE_ASCII;
  } else if (strcmp("bigint", type) == 0) {
    *value_type = CASS_VALUE_TYPE_BIGINT;
  } else if (strcmp("blob", type) == 0) {
    *value_type = CASS_VALUE_TYPE_BLOB;
  } else if (strcmp("boolean", type) == 0) {
    *value_type = CASS_VALUE_TYPE_BOOLEAN;
  } else if (strcmp("counter", type) == 0) {
    *value_type = CASS_VALUE_TYPE_COUNTER;
  } else if (strcmp("decimal", type) == 0) {
    *value_type = CASS_VALUE_TYPE_DECIMAL;
  } else if (strcmp("double", type) == 0) {
    *value_type = CASS_VALUE_TYPE_DOUBLE;
  } else if (strcmp("float", type) == 0) {
    *value_type = CASS_VALUE_TYPE_FLOAT;
  } else if (strcmp("int", type) == 0) {
    *value_type = CASS_VALUE_TYPE_INT;
  } else if (strcmp("text", type) == 0) {
    *value_type = CASS_VALUE_TYPE_TEXT;
  } else if (strcmp("timestamp", type) == 0) {
    *value_type = CASS_VALUE_TYPE_TIMESTAMP;
  } else if (strcmp("uuid", type) == 0) {
    *value_type = CASS_VALUE_TYPE_UUID;
  } else if (strcmp("varchar", type) == 0) {
    *value_type = CASS_VALUE_TYPE_VARCHAR;
  } else if (strcmp("varint", type) == 0) {
    *value_type = CASS_VALUE_TYPE_VARINT;
  } else if (strcmp("timeuuid", type) == 0) {
    *value_type = CASS_VALUE_TYPE_TIMEUUID;
  } else if (strcmp("inet", type) == 0) {
    *value_type = CASS_VALUE_TYPE_INET;
  } else {
    zend_throw_exception_ex(cassandra_invalid_argument_exception_ce, 0 TSRMLS_CC,
      "Unsupported type '%s'", type);
    return 0;
  }

  return 1;
}

const char*
php_cassandra_type_name(CassValueType value_type)
{
  switch (value_type) {
  case CASS_VALUE_TYPE_TEXT:
    return "text";
  case CASS_VALUE_TYPE_ASCII:
    return "ascii";
  case CASS_VALUE_TYPE_VARCHAR:
    return "varchar";
  case CASS_VALUE_TYPE_BIGINT:
    return "bigint";
  case CASS_VALUE_TYPE_BLOB:
    return "blob";
  case CASS_VALUE_TYPE_BOOLEAN:
    return "boolean";
  case CASS_VALUE_TYPE_COUNTER:
    return "counter";
  case CASS_VALUE_TYPE_DECIMAL:
    return "decimal";
  case CASS_VALUE_TYPE_DOUBLE:
    return "double";
  case CASS_VALUE_TYPE_FLOAT:
    return "float";
  case CASS_VALUE_TYPE_INT:
    return "int";
  case CASS_VALUE_TYPE_TIMESTAMP:
    return "timestamp";
  case CASS_VALUE_TYPE_UUID:
    return "uuid";
  case CASS_VALUE_TYPE_VARINT:
    return "varint";
  case CASS_VALUE_TYPE_TIMEUUID:
    return "timeuuid";
  case CASS_VALUE_TYPE_INET:
    return "inet";
  default:
    return "unknown";
  }
}

static int
php_cassandra_collection_append(CassCollection* collection, zval* value, CassValueType type TSRMLS_DC)
{
  int result = 1;
  cassandra_float*      float_number;
  cassandra_bigint*     bigint;
  cassandra_blob*       blob;
  cassandra_timestamp*  timestamp;
  cassandra_uuid*       uuid;
  cassandra_varint*     varint;
  cassandra_decimal*    decimal;
  cassandra_inet*       inet;
  size_t                size;
  cass_byte_t*          data;

  switch (type) {
  case CASS_VALUE_TYPE_TEXT:
  case CASS_VALUE_TYPE_ASCII:
  case CASS_VALUE_TYPE_VARCHAR:
    CHECK_ERROR(cass_collection_append_string_n(collection, Z_STRVAL_P(value), Z_STRLEN_P(value)));
    break;
  case CASS_VALUE_TYPE_BIGINT:
  case CASS_VALUE_TYPE_COUNTER:
    bigint = (cassandra_bigint*) zend_object_store_get_object(value TSRMLS_CC);
    CHECK_ERROR(cass_collection_append_int64(collection, bigint->value));
    break;
  case CASS_VALUE_TYPE_BLOB:
    blob = (cassandra_blob*) zend_object_store_get_object(value TSRMLS_CC);
    CHECK_ERROR(cass_collection_append_bytes(collection, blob->data, blob->size));
    break;
  case CASS_VALUE_TYPE_BOOLEAN:
    CHECK_ERROR(cass_collection_append_bool(collection, Z_BVAL_P(value)));
    break;
  case CASS_VALUE_TYPE_DOUBLE:
    CHECK_ERROR(cass_collection_append_double(collection, Z_DVAL_P(value)));
    break;
  case CASS_VALUE_TYPE_FLOAT:
    float_number = (cassandra_float*) zend_object_store_get_object(value TSRMLS_CC);
    CHECK_ERROR(cass_collection_append_float(collection, float_number->value));
    break;
  case CASS_VALUE_TYPE_INT:
    CHECK_ERROR(cass_collection_append_int32(collection, Z_LVAL_P(value)));
    break;
  case CASS_VALUE_TYPE_TIMESTAMP:
    timestamp = (cassandra_timestamp*) zend_object_store_get_object(value TSRMLS_CC);
    CHECK_ERROR(cass_collection_append_int64(collection, timestamp->timestamp));
    break;
  case CASS_VALUE_TYPE_UUID:
  case CASS_VALUE_TYPE_TIMEUUID:
    uuid = (cassandra_uuid*) zend_object_store_get_object(value TSRMLS_CC);
    CHECK_ERROR(cass_collection_append_uuid(collection, uuid->uuid));
    break;
  case CASS_VALUE_TYPE_VARINT:
    varint = (cassandra_varint*) zend_object_store_get_object(value TSRMLS_CC);
    data = (cass_byte_t*) export_twos_complement(varint->value, &size);
    CHECK_ERROR(cass_collection_append_bytes(collection, data, size));
    free(data);
    break;
  case CASS_VALUE_TYPE_DECIMAL:
    decimal = (cassandra_decimal*) zend_object_store_get_object(value TSRMLS_CC);
    data = (cass_byte_t*) export_twos_complement(decimal->value, &size);
    CHECK_ERROR(cass_collection_append_decimal(collection, data, size, decimal->scale));
    free(data);
    break;
  case CASS_VALUE_TYPE_INET:
    inet = (cassandra_inet*) zend_object_store_get_object(value TSRMLS_CC);
    CHECK_ERROR(cass_collection_append_inet(collection, inet->inet));
    break;
  default:
    zend_throw_exception_ex(cassandra_runtime_exception_ce, 0 TSRMLS_CC, "Unsupported collection type");
    return 0;
  }

  return result;
}

int
php_cassandra_collection_from_set(cassandra_set* set, CassCollection** collection_ptr TSRMLS_DC)
{
  int result = 1;
  CassCollection* collection = NULL;
  cassandra_type_set* type;
  cassandra_type* element_type;
  cassandra_set_entry* curr, * temp;

  collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, HASH_COUNT(set->entries));

  type = (cassandra_type_set*) zend_object_store_get_object(set->ztype TSRMLS_CC);
  element_type = (cassandra_type*) zend_object_store_get_object(type->element_type TSRMLS_CC);

  HASH_ITER(hh, set->entries, curr, temp) {
    if (php_cassandra_collection_append(collection, curr->element, element_type->type TSRMLS_CC)) {
      result = 0;
      break;
    }
  }

  if (result)
    *collection_ptr = collection;
  else
    cass_collection_free(collection);

  return result;
}

int
php_cassandra_collection_from_collection(cassandra_collection* coll, CassCollection** collection_ptr TSRMLS_DC)
{
  int result = 1;
  HashPointer ptr;
  zval** current;
  CassCollection* collection = NULL;

  zend_hash_get_pointer(&coll->values, &ptr);
  zend_hash_internal_pointer_reset(&coll->values);

  collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, zend_hash_num_elements(&coll->values));

  while (zend_hash_get_current_data(&coll->values, (void**) &current) == SUCCESS) {
    if (!php_cassandra_collection_append(collection, *current, coll->type TSRMLS_CC)) {
      result = 0;
      break;
    }
    zend_hash_move_forward(&coll->values);
  }

  zend_hash_set_pointer(&coll->values, &ptr);

  if (result)
    *collection_ptr = collection;
  else
    cass_collection_free(collection);

  return result;
}

int
php_cassandra_collection_from_map(cassandra_map* map, CassCollection** collection_ptr TSRMLS_DC)
{
  int result = 1;
  CassCollection* collection = NULL;
  cassandra_type_map* type;
  cassandra_type* key_type;
  cassandra_type* value_type;
  cassandra_map_entry* curr, * temp;

  collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, HASH_COUNT(map->entries));

  type = (cassandra_type_map*) zend_object_store_get_object(map->ztype TSRMLS_CC);
  key_type = (cassandra_type*) zend_object_store_get_object(type->key_type TSRMLS_CC);
  value_type = (cassandra_type*) zend_object_store_get_object(type->value_type TSRMLS_CC);

  HASH_ITER(hh, map->entries, curr, temp) {
    if (php_cassandra_collection_append(collection, curr->key, key_type->type TSRMLS_CC)) {
      result = 0;
      break;
    }
    if (php_cassandra_collection_append(collection, curr->value, value_type->type TSRMLS_CC)) {
      result = 0;
      break;
    }
  }

  if (result)
    *collection_ptr = collection;
  else
    cass_collection_free(collection);

  return result;
}
