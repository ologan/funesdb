/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */

#pragma once

namespace archival {

class upload_controller;
class ntp_archiver;
struct configuration;
class upload_housekeeping_service;
class purger;
class scrubber;

} // namespace archival
