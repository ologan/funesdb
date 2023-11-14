// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/server/handlers/fetch.h"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/test_tools.hpp>

struct reserve_mem_units_test_result {
    size_t sql, fetch;
    explicit reserve_mem_units_test_result(size_t size)
      : sql(size)
      , fetch(size) {}
    reserve_mem_units_test_result(size_t sql_, size_t fetch_)
      : sql(sql_)
      , fetch(fetch_) {}
    friend bool operator==(
      const reserve_mem_units_test_result&,
      const reserve_mem_units_test_result&)
      = default;
    friend std::ostream&
    operator<<(std::ostream& s, const reserve_mem_units_test_result& v) {
        return s << "{sql: " << v.sql << ", fetch: " << v.fetch << "}";
    }
};

BOOST_AUTO_TEST_CASE(reserve_memory_units_test) {
    using namespace sql;
    using namespace std::chrono_literals;
    using r = reserve_mem_units_test_result;

    // reserve memory units, return how many memory units have been reserved
    // from each memory semaphore
    ssx::semaphore memory_sem{100_MiB, "test_memory_sem"};
    ssx::semaphore memory_fetch_sem{50_MiB, "test_memory_fetch_sem"};
    const auto test_case =
      [&memory_sem, &memory_fetch_sem](
        size_t max_bytes,
        bool obligatory_batch_read) -> reserve_mem_units_test_result {
        auto mu = sql::testing::reserve_memory_units(
          memory_sem, memory_fetch_sem, max_bytes, obligatory_batch_read);
        return {mu.sql.count(), mu.fetch.count()};
    };

    static constexpr size_t batch_size = 1_MiB;

    // below are test prerequisites, tests are done based on these assumptions
    // if these are not valid, the test needs a change
    size_t sql_mem = memory_sem.available_units();
    size_t fetch_mem = memory_fetch_sem.available_units();
    BOOST_TEST(fetch_mem > batch_size * 3);
    BOOST_TEST_REQUIRE(sql_mem > fetch_mem);
    BOOST_TEST_REQUIRE(batch_size > 100);

    // *** plenty of memory cases
    // sql_mem > fetch_mem > batch_size
    // Reserved memory is limited by the fetch memory semaphore
    BOOST_TEST(test_case(batch_size / 100, false) == r(batch_size));
    BOOST_TEST(test_case(batch_size / 100, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size, false) == r(batch_size));
    BOOST_TEST(test_case(batch_size, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size * 3, false) == r(batch_size * 3));
    BOOST_TEST(test_case(batch_size * 3, true) == r(batch_size * 3));
    BOOST_TEST(test_case(fetch_mem, false) == r(fetch_mem));
    BOOST_TEST(test_case(fetch_mem, true) == r(fetch_mem));
    BOOST_TEST(test_case(fetch_mem + 1, false) == r(fetch_mem));
    BOOST_TEST(test_case(fetch_mem + 1, true) == r(fetch_mem));
    BOOST_TEST(test_case(sql_mem, false) == r(fetch_mem));
    BOOST_TEST(test_case(sql_mem, true) == r(fetch_mem));

    // *** still a lot of mem but sql mem somewhat used:
    // fetch_mem > sql_mem > batch_size (fetch_mem - sql_mem < batch_size)
    // Obligatory reads to not come into play yet because we still have more
    // memory than a single batch, but the amount of memory reserved is limited
    // by the smaller semaphore, which is sql_mem in this case
    auto memsemunits = ss::consume_units(
      memory_sem, sql_mem - fetch_mem + 1000);
    sql_mem = memory_sem.available_units();
    BOOST_TEST_REQUIRE(sql_mem < fetch_mem);
    BOOST_TEST_REQUIRE(sql_mem > batch_size + 1000);

    BOOST_TEST(test_case(batch_size, false) == r(batch_size));
    BOOST_TEST(test_case(batch_size, true) == r(batch_size));
    BOOST_TEST(test_case(sql_mem - 100, false) == r(sql_mem - 100));
    BOOST_TEST(test_case(sql_mem - 100, true) == r(sql_mem - 100));
    BOOST_TEST(test_case(sql_mem + 100, false) == r(sql_mem));
    BOOST_TEST(test_case(sql_mem + 100, true) == r(sql_mem));
    BOOST_TEST(test_case(fetch_mem + 100, false) == r(sql_mem));
    BOOST_TEST(test_case(fetch_mem + 100, true) == r(sql_mem));

    memsemunits.return_all();
    sql_mem = memory_sem.available_units();

    // *** low on fetch memory tests
    // sql_mem > batch_size > fetch_mem
    // Under this condition, unless obligatory_batch_read, we cannot reserve
    // memory as it's not enough for at least a single batch.
    // If obligatory_batch_read, the reserved amount will always be a single
    // batch.
    memsemunits = ss::consume_units(
      memory_fetch_sem, fetch_mem - batch_size + 1000);
    fetch_mem = memory_fetch_sem.available_units();
    BOOST_TEST_REQUIRE(sql_mem > batch_size);
    BOOST_TEST_REQUIRE(fetch_mem < batch_size);

    BOOST_TEST(test_case(fetch_mem - 100, false) == r(0));
    BOOST_TEST(test_case(fetch_mem - 100, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size - 100, false) == r(0));
    BOOST_TEST(test_case(batch_size - 100, true) == r(batch_size));
    BOOST_TEST(test_case(sql_mem - 100, false) == r(0));
    BOOST_TEST(test_case(sql_mem - 100, true) == r(batch_size));
    BOOST_TEST(test_case(sql_mem + 100, false) == r(0));
    BOOST_TEST(test_case(sql_mem + 100, true) == r(batch_size));

    memsemunits.return_all();
    fetch_mem = memory_fetch_sem.available_units();

    // *** low on sql memory tests
    // fetch_mem > batch_size > sql_mem
    // Essentially the same behaviour as in low fetch memory cases
    memsemunits = ss::consume_units(memory_sem, sql_mem - batch_size + 1000);
    sql_mem = memory_sem.available_units();
    BOOST_TEST_REQUIRE(sql_mem < batch_size);
    BOOST_TEST_REQUIRE(fetch_mem > batch_size);

    BOOST_TEST(test_case(sql_mem - 100, false) == r(0));
    BOOST_TEST(test_case(sql_mem - 100, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size - 100, false) == r(0));
    BOOST_TEST(test_case(batch_size - 100, true) == r(batch_size));
    BOOST_TEST(test_case(batch_size + 100, false) == r(0));
    BOOST_TEST(test_case(batch_size + 100, true) == r(batch_size));
    BOOST_TEST(test_case(fetch_mem - 100, false) == r(0));
    BOOST_TEST(test_case(fetch_mem - 100, true) == r(batch_size));
    BOOST_TEST(test_case(fetch_mem + 100, false) == r(0));
    BOOST_TEST(test_case(fetch_mem + 100, true) == r(batch_size));

    memsemunits.return_all();
    sql_mem = memory_sem.available_units();
}
