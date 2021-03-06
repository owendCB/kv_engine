/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "buckets.h"
#include "mc_time.h"
#include "stats.h"
#include "topkeys.h"
#include <memcached/dcp.h>
#include <memcached/engine.h>

Bucket::Bucket() = default;

bool Bucket::supports(cb::engine::Feature feature) {
    return supportedFeatures.find(feature) != supportedFeatures.end();
}

DcpIface* Bucket::getDcpIface() const {
    return bucketDcp;
}

EngineIface* Bucket::getEngine() const {
    return engine;
}

void Bucket::setEngine(EngineIface* engine) {
    Bucket::engine = engine;
    bucketDcp = dynamic_cast<DcpIface*>(engine);
}

namespace BucketValidator {
    bool validateBucketName(const std::string& name, std::string& errors) {
        if (name.empty()) {
            errors.assign("BucketValidator::validateBucketName: "
                              "Name can't be empty");
            return false;
        }

        if (name.length() > MAX_BUCKET_NAME_LENGTH) {
            errors.assign("BucketValidator::validateBucketName: Name"
                              " too long (exceeds " +
                          std::to_string(MAX_BUCKET_NAME_LENGTH) +
                          ")");
            return false;
        }

        // Verify that the bucket name only consists of legal characters
        for (const uint8_t ii : name) {
            if (!(isupper(ii) || islower(ii) || isdigit(ii))) {
                switch (ii) {
                case '_':
                case '-':
                case '.':
                case '%':
                    break;
                default:
                    errors.assign("BucketValidator::validateBucketName: "
                                      "name contains invalid characters");
                    return false;
                }
            }
        }

        return true;
    }

    bool validateBucketType(const Bucket::Type& type, std::string& errors) {
        if (type == Bucket::Type::Unknown) {
            errors.assign("BucketValidator::validateBucketType: "
                              "Unsupported bucket type");
            return false;
        }

        return true;
    }
}

std::string to_string(Bucket::Type type) {
    switch (type) {
    case Bucket::Type::Memcached:
        return "Memcached";
    case Bucket::Type::Couchstore:
        return "Couchstore";
    case Bucket::Type::EWouldBlock:
        return "EWouldBlock";
    case Bucket::Type::NoBucket:
        return "No Bucket";
    case Bucket::Type::Unknown:
        return "Uknown";
    }
    throw std::logic_error("Invalid bucket type: " + std::to_string(int(type)));
}

std::string to_string(Bucket::State state) {
    switch (state) {
    case Bucket::State::None:
        return "none";
    case Bucket::State::Creating:
        return "creating";
    case Bucket::State::Initializing:
        return "initializing";
    case Bucket::State::Ready:
        return "ready";
    case Bucket::State::Stopping:
        return "stopping";
    case Bucket::State::Destroying:
        return "destroying";
    }
    throw std::invalid_argument("Invalid bucket state: " +
                                std::to_string(int(state)));
}