/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */

#pragma once

#include "outcome.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <krb5/krb5.h>

namespace security::krb5 {

namespace impl {

struct error_impl {
    ::krb5_error_code ec{0};
    ss::sstring msg;

    friend std::ostream& operator<<(std::ostream& os, const error_impl& impl) {
        os << impl.msg << " (ec: " << impl.ec << ")";
        return os;
    }
};

} // namespace impl

template<typename R>
using result = result<R, impl::error_impl>;

class context {
public:
    static result<context> create() noexcept;

    result<ss::sstring> get_default_realm() const noexcept;

private:
    explicit context(::krb5_context ctx)
      : _ctx(ctx, &::krb5_free_context) {}

    std::unique_ptr<::_krb5_context, void (*)(::krb5_context)> _ctx;
};

} // namespace security::krb5
