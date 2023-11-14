/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "funesproxy/schema_registry/types.h"
#include "wasm/ffi.h"
#include "wasm/schema_registry.h"

namespace wasm {

/**
 * The WASM module for funes schema registry.
 *
 * This provides an ABI to WASM guests for the wasm::schema_registry wrapper
 */
class schema_registry_module {
public:
    explicit schema_registry_module(schema_registry*);
    schema_registry_module(const schema_registry_module&) = delete;
    schema_registry_module& operator=(const schema_registry_module&) = delete;
    schema_registry_module(schema_registry_module&&) = default;
    schema_registry_module& operator=(schema_registry_module&&) = default;
    ~schema_registry_module() = default;

    static constexpr std::string_view name = "funes_schema_registry";

    // Start ABI exports

    ss::future<int32_t> get_schema_definition_len(
      funesproxy::schema_registry::schema_id, uint32_t*);

    ss::future<int32_t> get_schema_definition(
      funesproxy::schema_registry::schema_id, ffi::array<uint8_t>);

    ss::future<int32_t> get_subject_schema_len(
      funesproxy::schema_registry::subject,
      funesproxy::schema_registry::schema_version,
      uint32_t*);

    ss::future<int32_t> get_subject_schema(
      funesproxy::schema_registry::subject,
      funesproxy::schema_registry::schema_version,
      ffi::array<uint8_t>);

    ss::future<int32_t> create_subject_schema(
      funesproxy::schema_registry::subject,
      ffi::array<uint8_t>,
      funesproxy::schema_registry::schema_id*);

    // End ABI exports

private:
    schema_registry* _sr;
};
} // namespace wasm
