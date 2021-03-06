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

#include <nlohmann/json.hpp>
#include <platform/base64.h>
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <platform/timeutils.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <gsl/gsl>
#include <system_error>

#include "log_macros.h"
#include "opentracing_config.h"
#include "settings.h"
#include "ssl_utils.h"

#include <mcbp/mcbp.h>
#include <utilities/json_utilities.h>
#include <utilities/logtags.h>

// the global entry of the settings object
Settings settings;


/**
 * Initialize all members to "null" to preserve backwards
 * compatibility with the previous versions.
 */
Settings::Settings()
    : num_threads(0),
      bio_drain_buffer_sz(0),
      datatype_json(false),
      datatype_snappy(false),
      reqs_per_event_high_priority(0),
      reqs_per_event_med_priority(0),
      reqs_per_event_low_priority(0),
      default_reqs_per_event(00),
      max_packet_size(0),
      topkeys_size(0) {
    verbose.store(0);
    connection_idle_time.reset();
    dedupe_nmvb_maps.store(false);
    xattr_enabled.store(false);
    privilege_debug.store(false);
    collections_enabled.store(true);

    memset(&has, 0, sizeof(has));
}

Settings::Settings(const nlohmann::json& json) : Settings() {
    reconfigure(json);
}

/**
 * Handle deprecated tags in the settings by simply ignoring them
 */
static void ignore_entry(Settings&, const nlohmann::json&) {
}

enum class FileError {
    Missing,
    Empty,
    Invalid
};

static void throw_file_exception(const std::string &key,
                                 const std::string& filename,
                                 FileError reason,
                                 const std::string& extra_reason = std::string()) {
    std::string message("'" + key + "': '" + filename + "'");
    if (reason == FileError::Missing) {
        throw std::system_error(
                std::make_error_code(std::errc::no_such_file_or_directory),
                message);
    } else if (reason == FileError::Empty) {
        throw std::invalid_argument(message + " is empty ");
    } else if (reason == FileError::Invalid) {
        std::string extra;
        if (!extra_reason.empty()) {
            extra = " (" + extra_reason + ")";
        }
        throw std::invalid_argument(message + " is badly formatted: " +
                                    extra_reason);
    } else {
        throw std::runtime_error(message);
    }
}

static void throw_missing_file_exception(const std::string& key,
                                         const std::string& filename) {
    throw_file_exception(key, filename, FileError::Missing);
}

/**
 * Handle the "rbac_file" tag in the settings
 *
 *  The value must be a string that points to a file that must exist
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_rbac_file(Settings& s, const nlohmann::json& obj) {
    std::string file = obj.get<std::string>();

    if (!cb::io::isFile(file)) {
        throw_missing_file_exception("rbac_file", file);
    }

    s.setRbacFile(file);
}

/**
 * Handle the "privilege_debug" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_privilege_debug(Settings& s, const nlohmann::json& obj) {
    s.setPrivilegeDebug(obj.get<bool>());
}

/**
 * Handle the "audit_file" tag in the settings
 *
 *  The value must be a string that points to a file that must exist
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_audit_file(Settings& s, const nlohmann::json& obj) {
    std::string file = obj.get<std::string>();

    if (!cb::io::isFile(file)) {
        throw_missing_file_exception("audit_file", file);
    }

    s.setAuditFile(file);
}

static void handle_error_maps_dir(Settings& s, const nlohmann::json& obj) {
    s.setErrorMapsDir(obj.get<std::string>());
}

/**
 * Handle the "threads" tag in the settings
 *
 *  The value must be an integer value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_threads(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError("\"threads\" must be an unsigned int");
    }
    s.setNumWorkerThreads(gsl::narrow_cast<size_t>(obj.get<unsigned int>()));
}

/**
 * Handle the "topkeys_enabled" tag in the settings
 *
 *  The value must be a  value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_topkeys_enabled(Settings& s, const nlohmann::json& obj) {
    s.setTopkeysEnabled(obj.get<bool>());
}

static void handle_scramsha_fallback_salt(Settings& s,
                                          const nlohmann::json& obj) {
    // Try to base64 decode it to validate that it is a legal value..
    std::string salt = obj.get<std::string>();
    cb::base64::decode(salt);
    s.setScramshaFallbackSalt(salt);
}

static void handle_external_auth_service(Settings& s,
                                         const nlohmann::json& obj) {
    s.setExternalAuthServiceEnabled(obj.get<bool>());
}

static void handle_active_external_users_push_interval(
        Settings& s, const nlohmann::json& obj) {
    switch (obj.type()) {
    case nlohmann::json::value_t::number_unsigned:
        s.setActiveExternalUsersPushInterval(
                std::chrono::seconds(obj.get<int>()));
        break;
    case nlohmann::json::value_t::string:
        s.setActiveExternalUsersPushInterval(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        cb::text2time(obj.get<std::string>())));
        break;
    default:
        cb::throwJsonTypeError(R"("active_external_users_push_interval" must
                                be a number or string)");
    }
}

/**
 * Handle the "tracing_enabled" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_tracing_enabled(Settings& s, const nlohmann::json& obj) {
    s.setTracingEnabled(obj.get<bool>());
}

/**
 * Handle the "stdin_listener" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_stdin_listener(Settings& s, const nlohmann::json& obj) {
    s.setStdinListenerEnabled(obj.get<bool>());
}

/**
 * Handle "default_reqs_per_event", "reqs_per_event_high_priority",
 * "reqs_per_event_med_priority" and "reqs_per_event_low_priority" tag in
 * the settings
 *
 *  The value must be a integer value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_reqs_event(Settings& s,
                              const nlohmann::json& obj,
                              EventPriority priority,
                              const std::string& msg) {
    // Throw if not an unsigned int. Bool values can be converted to an int
    // in an nlohmann::json.get<unsigned int>() so we need to check this
    // explicitly.
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(msg + " must be an unsigned int");
    }

    s.setRequestsPerEventNotification(gsl::narrow<int>(obj.get<unsigned int>()),
                                      priority);
}

static void handle_default_reqs_event(Settings& s, const nlohmann::json& obj) {
    handle_reqs_event(s, obj, EventPriority::Default, "default_reqs_per_event");
}

static void handle_high_reqs_event(Settings& s, const nlohmann::json& obj) {
    handle_reqs_event(
            s, obj, EventPriority::High, "reqs_per_event_high_priority");
}

static void handle_med_reqs_event(Settings& s, const nlohmann::json& obj) {
    handle_reqs_event(
            s, obj, EventPriority::Medium, "reqs_per_event_med_priority");
}

static void handle_low_reqs_event(Settings& s, const nlohmann::json& obj) {
    handle_reqs_event(
            s, obj, EventPriority::Low, "reqs_per_event_low_priority");
}

/**
 * Handle the "verbosity" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_verbosity(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError("\"verbosity\" must be an unsigned int");
    }
    s.setVerbose(gsl::narrow<int>(obj.get<unsigned int>()));
}

/**
 * Handle the "connection_idle_time" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_connection_idle_time(Settings& s,
                                        const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                "\"connection_idle_time\" must be an unsigned "
                "int");
    }
    s.setConnectionIdleTime(obj.get<unsigned int>());
}

/**
 * Handle the "bio_drain_buffer_sz" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_bio_drain_buffer_sz(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                "\"bio_drain_buffer_sz\" must be an unsigned "
                "int");
    }
    s.setBioDrainBufferSize(gsl::narrow<unsigned int>(obj.get<unsigned int>()));
}

/**
 * Handle the "datatype_snappy" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_datatype_json(Settings& s, const nlohmann::json& obj) {
    s.setDatatypeJsonEnabled(obj.get<bool>());
}

/**
 * Handle the "datatype_snappy" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_datatype_snappy(Settings& s, const nlohmann::json& obj) {
    s.setDatatypeSnappyEnabled(obj.get<bool>());
}

/**
 * Handle the "root" tag in the settings
 *
 * The value must be a string that points to a directory that must exist
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_root(Settings& s, const nlohmann::json& obj) {
    std::string dir = obj.get<std::string>();

    if (!cb::io::isDirectory(dir)) {
        throw_missing_file_exception("root", dir);
    }

    s.setRoot(dir);
}

/**
 * Handle the "ssl_cipher_list" tag in the settings
 *
 * The value must be a string
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_ssl_cipher_list(Settings& s, const nlohmann::json& obj) {
    s.setSslCipherList(obj.get<std::string>());
}

static void handle_ssl_cipher_order(Settings& s, const nlohmann::json& obj) {
    s.setSslCipherOrder(obj.get<bool>());
}

/**
 * Handle the "ssl_minimum_protocol" tag in the settings
 *
 * The value must be a string containing one of the following:
 *    tlsv1, tlsv1.1, tlsv1_1, tlsv1.2, tlsv1_2, tlsv1.3, tlsv1_3
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_ssl_minimum_protocol(Settings& s,
                                        const nlohmann::json& obj) {
    std::string protocol = obj.get<std::string>();
    try {
        decode_ssl_protocol(protocol);
    } catch (const std::exception& e) {
        throw std::invalid_argument(
            "\"ssl_minimum_protocol\"" + std::string(e.what()));
    }
    s.setSslMinimumProtocol(protocol);
}

/**
 * Handle the "get_max_packet_size" tag in the settings
 *
 *  The value must be a numeric value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_max_packet_size(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError("\"max_packet_size\" must be an unsigned int");
    }
    s.setMaxPacketSize(gsl::narrow<uint32_t>(obj.get<unsigned int>()) * 1024 *
                       1024);
}

static void handle_max_connections(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                R"("max_connections" must be a positive number)");
    }
    s.setMaxConnections(obj.get<size_t>());
}

static void handle_system_connections(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_number_unsigned()) {
        cb::throwJsonTypeError(
                R"("system_connections" must be a positive number)");
    }
    s.setSystemConnections(obj.get<size_t>());
}

/**
 * Handle the "sasl_mechanisms" tag in the settings
 *
 * The value must be a string
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_sasl_mechanisms(Settings& s, const nlohmann::json& obj) {
    s.setSaslMechanisms(obj.get<std::string>());
}

/**
 * Handle the "ssl_sasl_mechanisms" tag in the settings
 *
 * The value must be a string
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_ssl_sasl_mechanisms(Settings& s, const nlohmann::json& obj) {
    s.setSslSaslMechanisms(obj.get<std::string>());
}

/**
 * Handle the "dedupe_nmvb_maps" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_dedupe_nmvb_maps(Settings& s, const nlohmann::json& obj) {
    s.setDedupeNmvbMaps(obj.get<bool>());
}

/**
 * Handle the "xattr_enabled" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_xattr_enabled(Settings& s, const nlohmann::json& obj) {
    s.setXattrEnabled(obj.get<bool>());
}

/**
 * Handle the "client_cert_auth" tag in the settings
 *
 *  The value must be a string value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_client_cert_auth(Settings& s, const nlohmann::json& obj) {
    auto config = cb::x509::ClientCertConfig::create(obj);
    s.reconfigureClientCertAuth(config);
}

/**
 * Handle the "collections_enabled" tag in the settings
 *
 *  The value must be a boolean value
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_collections_enabled(Settings& s, const nlohmann::json& obj) {
    s.setCollectionsPrototype(obj.get<bool>());
}

static void handle_opcode_attributes_override(Settings& s,
                                              const nlohmann::json& obj) {
    if (obj.type() != nlohmann::json::value_t::object) {
        throw std::invalid_argument(
                R"("opcode_attributes_override" must be an object)");
    }
    s.setOpcodeAttributesOverride(obj.dump());
}

static void handle_extensions(Settings& s, const nlohmann::json& obj) {
    LOG_INFO("Extensions ignored");
}

static void handle_logger(Settings& s, const nlohmann::json& obj) {
    if (!obj.is_object()) {
        cb::throwJsonTypeError(R"("opcode_attributes_override" must be an
                              object)");
    }
    cb::logger::Config config(obj);
    s.setLoggerConfig(config);
}

/**
 * Handle the "interfaces" tag in the settings
 *
 *  The value must be an array
 *
 * @param s the settings object to update
 * @param obj the object in the configuration
 */
static void handle_interfaces(Settings& s, const nlohmann::json& obj) {
    if (obj.type() != nlohmann::json::value_t::array) {
        cb::throwJsonTypeError("\"interfaces\" must be an array");
    }

    for (const auto& o : obj) {
        if (o.type() != nlohmann::json::value_t::object) {
            throw std::invalid_argument(
                    "Elements in the \"interfaces\" array must be objects");
        }
        NetworkInterface ifc(o);
        s.addInterface(ifc);
    }
}

static void handle_breakpad(Settings& s, const nlohmann::json& obj) {
    cb::breakpad::Settings breakpad(obj);
    s.setBreakpadSettings(breakpad);
}

static void handle_opentracing(Settings& s, const nlohmann::json& obj) {
    s.setOpenTracingConfig(std::make_shared<OpenTracingConfig>(obj));
}

void Settings::reconfigure(const nlohmann::json& json) {
    // Nuke the default interface added to the system in settings_init and
    // use the ones in the configuration file.. (this is a bit messy)
    interfaces.clear();

    struct settings_config_tokens {
        /**
         * The key in the configuration
         */
        std::string key;

        /**
         * A callback method used by the Settings object when we're parsing
         * the config attributes.
         *
         * @param settings the Settings object to update
         * @param obj the current object in the configuration we're looking at
         * @throws nlohmann::json::exception if the json cannot be parsed
         * @throws std::invalid_argument for other json input errors
         */
        void (*handler)(Settings& settings, const nlohmann::json& obj);
    };

    std::vector<settings_config_tokens> handlers = {
            {"admin", ignore_entry},
            {"rbac_file", handle_rbac_file},
            {"privilege_debug", handle_privilege_debug},
            {"audit_file", handle_audit_file},
            {"error_maps_dir", handle_error_maps_dir},
            {"threads", handle_threads},
            {"interfaces", handle_interfaces},
            {"extensions", handle_extensions},
            {"logger", handle_logger},
            {"default_reqs_per_event", handle_default_reqs_event},
            {"reqs_per_event_high_priority", handle_high_reqs_event},
            {"reqs_per_event_med_priority", handle_med_reqs_event},
            {"reqs_per_event_low_priority", handle_low_reqs_event},
            {"verbosity", handle_verbosity},
            {"connection_idle_time", handle_connection_idle_time},
            {"bio_drain_buffer_sz", handle_bio_drain_buffer_sz},
            {"datatype_json", handle_datatype_json},
            {"datatype_snappy", handle_datatype_snappy},
            {"root", handle_root},
            {"ssl_cipher_list", handle_ssl_cipher_list},
            {"ssl_cipher_order", handle_ssl_cipher_order},
            {"ssl_minimum_protocol", handle_ssl_minimum_protocol},
            {"breakpad", handle_breakpad},
            {"max_packet_size", handle_max_packet_size},
            {"max_connections", handle_max_connections},
            {"system_connections", handle_system_connections},
            {"sasl_mechanisms", handle_sasl_mechanisms},
            {"ssl_sasl_mechanisms", handle_ssl_sasl_mechanisms},
            {"stdin_listener", handle_stdin_listener},
            {"dedupe_nmvb_maps", handle_dedupe_nmvb_maps},
            {"xattr_enabled", handle_xattr_enabled},
            {"client_cert_auth", handle_client_cert_auth},
            {"collections_enabled", handle_collections_enabled},
            {"opcode_attributes_override", handle_opcode_attributes_override},
            {"topkeys_enabled", handle_topkeys_enabled},
            {"tracing_enabled", handle_tracing_enabled},
            {"scramsha_fallback_salt", handle_scramsha_fallback_salt},
            {"external_auth_service", handle_external_auth_service},
            {"active_external_users_push_interval",
             handle_active_external_users_push_interval},
            {"opentracing", handle_opentracing}};

    for (const auto& obj : json.items()) {
        bool found = false;
        for (auto& handler : handlers) {
            if (handler.key == obj.key()) {
                handler.handler(*this, obj.value());
                found = true;
                break;
            }
        }

        if (!found) {
            LOG_WARNING(R"(Unknown key "{}" in config ignored.)", obj.key());
        }
    }
}

void Settings::setOpcodeAttributesOverride(
        const std::string& opcode_attributes_override) {
    if (!opcode_attributes_override.empty()) {
        // Verify the content...
        cb::mcbp::sla::reconfigure(
                nlohmann::json::parse(opcode_attributes_override), false);
    }

    {
        std::lock_guard<std::mutex> guard(
                Settings::opcode_attributes_override.mutex);
        Settings::opcode_attributes_override.value = opcode_attributes_override;
        has.opcode_attributes_override = true;
    }
    notify_changed("opcode_attributes_override");
}

void Settings::updateSettings(const Settings& other, bool apply) {
    if (other.has.rbac_file) {
        if (other.rbac_file != rbac_file) {
            throw std::invalid_argument("rbac_file can't be changed dynamically");
        }
    }
    if (other.has.threads) {
        if (other.num_threads != num_threads) {
            throw std::invalid_argument("threads can't be changed dynamically");
        }
    }

    if (other.has.audit) {
        if (other.audit_file != audit_file) {
            throw std::invalid_argument("audit can't be changed dynamically");
        }
    }
    if (other.has.bio_drain_buffer_sz) {
        if (other.bio_drain_buffer_sz != bio_drain_buffer_sz) {
            throw std::invalid_argument(
                "bio_drain_buffer_sz can't be changed dynamically");
        }
    }
    if (other.has.datatype_json) {
        if (other.datatype_json != datatype_json) {
            throw std::invalid_argument(
                    "datatype_json can't be changed dynamically");
        }
    }
    if (other.has.root) {
        if (other.root != root) {
            throw std::invalid_argument("root can't be changed dynamically");
        }
    }
    if (other.has.topkeys_size) {
        if (other.topkeys_size != topkeys_size) {
            throw std::invalid_argument(
                "topkeys_size can't be changed dynamically");
        }
    }

    if (other.has.interfaces) {
        if (other.interfaces.size() != interfaces.size()) {
            throw std::invalid_argument(
                "interfaces can't be changed dynamically");
        }

        // validate that we haven't changed stuff in the entries
        auto total = interfaces.size();
        for (std::vector<NetworkInterface>::size_type ii = 0; ii < total;
             ++ii) {
            const auto& i1 = interfaces[ii];
            const auto& i2 = other.interfaces[ii];

            if (i1.port == 0 || i2.port == 0) {
                // we can't look at dynamic ports...
                continue;
            }

            // the following fields can't change
            if ((i1.host != i2.host) || (i1.port != i2.port) ||
                (i1.ipv4 != i2.ipv4) || (i1.ipv6 != i2.ipv6)) {
                throw std::invalid_argument(
                    "interfaces can't be changed dynamically");
            }
        }
    }

    if (other.has.stdin_listener) {
        if (other.stdin_listener.load() != stdin_listener.load()) {
            throw std::invalid_argument(
                    "stdin_listener can't be changed dynamically");
        }
    }

    if (other.has.logger) {
        if (other.logger_settings != logger_settings)
            throw std::invalid_argument(
                    "logger configuration can't be changed dynamically");
    }

    if (other.has.error_maps) {
        if (other.error_maps_dir != error_maps_dir) {
            throw std::invalid_argument(
                    "error_maps_dir can't be changed dynamically");
        }
    }

    // All non-dynamic settings has been validated. If we're not supposed
    // to update anything we can bail out.
    if (!apply) {
        return;
    }

    // Ok, go ahead and update the settings!!
    if (other.has.datatype_snappy) {
        if (other.datatype_snappy != datatype_snappy) {
            std::string curr_val_str = datatype_snappy ? "true" : "false";
            std::string other_val_str = other.datatype_snappy ? "true" : "false";
            LOG_INFO("Change datatype_snappy from {} to {}",
                     curr_val_str,
                     other_val_str);
            setDatatypeSnappyEnabled(other.datatype_snappy);
        }
    }

    if (other.has.verbose) {
        if (other.verbose != verbose) {
            LOG_INFO("Change verbosity level from {} to {}",
                     verbose.load(),
                     other.verbose.load());
            setVerbose(other.verbose.load());
        }
    }

    if (other.has.reqs_per_event_high_priority) {
        if (other.reqs_per_event_high_priority !=
            reqs_per_event_high_priority) {
            LOG_INFO("Change high priority iterations per event from {} to {}",
                     reqs_per_event_high_priority,
                     other.reqs_per_event_high_priority);
            setRequestsPerEventNotification(other.reqs_per_event_high_priority,
                                            EventPriority::High);
        }
    }
    if (other.has.reqs_per_event_med_priority) {
        if (other.reqs_per_event_med_priority != reqs_per_event_med_priority) {
            LOG_INFO(
                    "Change medium priority iterations per event from {} to {}",
                    reqs_per_event_med_priority,
                    other.reqs_per_event_med_priority);
            setRequestsPerEventNotification(other.reqs_per_event_med_priority,
                                            EventPriority::Medium);
        }
    }
    if (other.has.reqs_per_event_low_priority) {
        if (other.reqs_per_event_low_priority != reqs_per_event_low_priority) {
            LOG_INFO("Change low priority iterations per event from {} to {}",
                     reqs_per_event_low_priority,
                     other.reqs_per_event_low_priority);
            setRequestsPerEventNotification(other.reqs_per_event_low_priority,
                                            EventPriority::Low);
        }
    }
    if (other.has.default_reqs_per_event) {
        if (other.default_reqs_per_event != default_reqs_per_event) {
            LOG_INFO("Change default iterations per event from {} to {}",
                     default_reqs_per_event,
                     other.default_reqs_per_event);
            setRequestsPerEventNotification(other.default_reqs_per_event,
                                            EventPriority::Default);
        }
    }
    if (other.has.connection_idle_time) {
        if (other.connection_idle_time != connection_idle_time) {
            LOG_INFO("Change connection idle time from {} to {}",
                     connection_idle_time.load(),
                     other.connection_idle_time.load());
            setConnectionIdleTime(other.connection_idle_time);
        }
    }
    if (other.has.max_packet_size) {
        if (other.max_packet_size != max_packet_size) {
            LOG_INFO("Change max packet size from {} to {}",
                     max_packet_size,
                     other.max_packet_size);
            setMaxPacketSize(other.max_packet_size);
        }
    }
    if (other.has.ssl_cipher_list) {
        if (other.ssl_cipher_list != ssl_cipher_list) {
            // this isn't safe!! an other thread could call stats settings
            // which would cause this to crash...
            LOG_INFO(
                    R"(Change SSL Cipher list from "{}" to "{}")",
                    ssl_cipher_list,
                    other.ssl_cipher_list);
            setSslCipherList(other.ssl_cipher_list);
        }
    }

    if (other.has.ssl_cipher_order) {
        if (other.ssl_cipher_order != ssl_cipher_order) {
            LOG_INFO(R"(Change SSL Cipher order from "{}" to "{}")",
                     ssl_cipher_order ? "enabled" : "disabled",
                     other.ssl_cipher_order ? "enabled" : "disabled");
            setSslCipherOrder(other.ssl_cipher_order);
        }
    }

    if (other.has.client_cert_auth) {
        const auto m = client_cert_mapper.to_string();
        const auto o = other.client_cert_mapper.to_string();

        if (m != o) {
            LOG_INFO(
                    R"(Change SSL client auth from "{}" to "{}")", m, o);
            // TODO MB-30041: Remove when we migrate settings
            nlohmann::json json = nlohmann::json::parse(o);
            auto config = cb::x509::ClientCertConfig::create(json);
            reconfigureClientCertAuth(config);
        }
    }
    if (other.has.ssl_minimum_protocol) {
        if (other.ssl_minimum_protocol != ssl_minimum_protocol) {
            // this isn't safe!! an other thread could call stats settings
            // which would cause this to crash...
            LOG_INFO(
                    R"(Change SSL minimum protocol from "{}" to "{}")",
                    ssl_minimum_protocol,
                    other.ssl_minimum_protocol);
            setSslMinimumProtocol(other.ssl_minimum_protocol);
        }
    }
    if (other.has.dedupe_nmvb_maps) {
        if (other.dedupe_nmvb_maps != dedupe_nmvb_maps) {
            LOG_INFO("{} deduplication of NMVB maps",
                     other.dedupe_nmvb_maps.load() ? "Enable" : "Disable");
            setDedupeNmvbMaps(other.dedupe_nmvb_maps.load());
        }
    }

    if (other.has.max_connections) {
        if (other.max_connections != max_connections) {
            LOG_INFO(R"(Change max connections from {} to {})",
                     max_connections,
                     other.max_connections);
            setMaxConnections(other.max_connections);
        }
    }

    if (other.has.system_connections) {
        if (other.system_connections != system_connections) {
            LOG_INFO(R"(Change max connections from {} to {})",
                     system_connections,
                     other.system_connections);
            setSystemConnections(other.system_connections);
        }
    }

    if (other.has.xattr_enabled) {
        if (other.xattr_enabled != xattr_enabled) {
            LOG_INFO("{} XATTR",
                     other.xattr_enabled.load() ? "Enable" : "Disable");
            setXattrEnabled(other.xattr_enabled.load());
        }
    }

    if (other.has.collections_enabled) {
        if (other.collections_enabled != collections_enabled) {
            LOG_INFO("{} collections_enabled",
                     other.collections_enabled.load() ? "Enable" : "Disable");
            setCollectionsPrototype(other.collections_enabled.load());
        }
    }

    if (other.has.interfaces) {
        // validate that we haven't changed stuff in the entries
        auto total = interfaces.size();
        bool changed = false;
        for (std::vector<NetworkInterface>::size_type ii = 0; ii < total;
             ++ii) {
            auto& i1 = interfaces[ii];
            const auto& i2 = other.interfaces[ii];

            if (i1.port == 0 || i2.port == 0) {
                // we can't look at dynamic ports...
                continue;
            }

            if (i2.tcp_nodelay != i1.tcp_nodelay) {
                LOG_INFO("{} TCP NODELAY for {}:{}",
                         i2.tcp_nodelay ? "Enable" : "Disable",
                         i1.host,
                         i1.port);
                i1.tcp_nodelay = i2.tcp_nodelay;
                changed = true;
            }

            if (i2.ssl.cert != i1.ssl.cert) {
                LOG_INFO("Change SSL Certificiate for {}:{} from {} to {}",
                         i1.host,
                         i1.port,
                         i1.ssl.cert,
                         i2.ssl.cert);
                i1.ssl.cert.assign(i2.ssl.cert);
                changed = true;
            }

            if (i2.ssl.key != i1.ssl.key) {
                LOG_INFO("Change SSL Key for {}:{} from {} to {}",
                         i1.host,
                         i1.port,
                         i1.ssl.key,
                         i2.ssl.key);
                i1.ssl.key.assign(i2.ssl.key);
                changed = true;
            }
        }

        if (changed) {
            notify_changed("interfaces");
        }
    }

    if (other.has.breakpad) {
        bool changed = false;
        auto& b1 = breakpad;
        const auto& b2 = other.breakpad;

        if (b2.enabled != b1.enabled) {
            LOG_INFO("{} breakpad", b2.enabled ? "Enable" : "Disable");
            b1.enabled = b2.enabled;
            changed = true;
        }

        if (b2.minidump_dir != b1.minidump_dir) {
            LOG_INFO(
                    R"(Change minidump directory from "{}" to "{}")",
                    b1.minidump_dir,
                    b2.minidump_dir);
            b1.minidump_dir = b2.minidump_dir;
            changed = true;
        }

        if (b2.content != b1.content) {
            LOG_INFO("Change minidump content from {} to {}",
                     to_string(b1.content),
                     to_string(b2.content));
            b1.content = b2.content;
            changed = true;
        }

        if (changed) {
            notify_changed("breakpad");
        }
    }

    if (other.has.privilege_debug) {
        if (other.privilege_debug != privilege_debug) {
            bool value = other.isPrivilegeDebug();
            LOG_INFO("{} privilege debug", value ? "Enable" : "Disable");
            setPrivilegeDebug(value);
        }
    }

    if (other.has.opcode_attributes_override) {
        auto current = getOpcodeAttributesOverride();
        auto proposed = other.getOpcodeAttributesOverride();

        if (proposed != current) {
            LOG_INFO(
                    R"(Change opcode attributes from "{}" to "{}")",
                    current,
                    proposed);
            setOpcodeAttributesOverride(proposed);
        }
    }

    if (other.has.topkeys_enabled) {
        if (other.isTopkeysEnabled() != isTopkeysEnabled()) {
            LOG_INFO("{} topkeys support",
                     other.isTopkeysEnabled() ? "Enable" : "Disable");
        }
        setTopkeysEnabled(other.isTopkeysEnabled());
    }

    if (other.has.tracing_enabled) {
        if (other.isTracingEnabled() != isTracingEnabled()) {
            LOG_INFO("{} tracing support",
                     other.isTracingEnabled() ? "Enable" : "Disable");
        }
        setTracingEnabled(other.isTracingEnabled());
    }

    if (other.has.scramsha_fallback_salt) {
        const auto o = other.getScramshaFallbackSalt();
        const auto m = getScramshaFallbackSalt();

        if (o != m) {
            LOG_INFO(R"(Change scram fallback salt from {} to {})",
                     cb::UserDataView(m),
                     cb::UserDataView(o));
            setScramshaFallbackSalt(o);
        }
    }

    if (other.has.sasl_mechanisms) {
        auto mine = getSaslMechanisms();
        auto others = other.getSaslMechanisms();
        if (mine != others) {
            LOG_INFO(
                    R"(Change SASL mechanisms on normal connections from "{}" to "{}")",
                    mine,
                    others);
            setSaslMechanisms(others);
        }
    }

    if (other.has.ssl_sasl_mechanisms) {
        auto mine = getSslSaslMechanisms();
        auto others = other.getSslSaslMechanisms();
        if (mine != others) {
            LOG_INFO(
                    R"(Change SASL mechanisms on SSL connections from "{}" to "{}")",
                    mine,
                    others);
            setSslSaslMechanisms(others);
        }
    }

    if (other.has.external_auth_service) {
        if (isExternalAuthServiceEnabled() !=
            other.isExternalAuthServiceEnabled()) {
            LOG_INFO(
                    R"(Change external authentication service from "{}" to "{}")",
                    isExternalAuthServiceEnabled() ? "enabled" : "disabled",
                    other.isExternalAuthServiceEnabled() ? "enabled"
                                                         : "disabled");
            setExternalAuthServiceEnabled(other.isExternalAuthServiceEnabled());
        }
    }

    if (other.has.active_external_users_push_interval) {
        if (getActiveExternalUsersPushInterval() !=
            other.getActiveExternalUsersPushInterval()) {
            LOG_INFO(
                    R"(Change push interval for external users list from {}s to {}s)",
                    std::chrono::duration_cast<std::chrono::seconds>(
                            getActiveExternalUsersPushInterval())
                            .count(),
                    std::chrono::duration_cast<std::chrono::seconds>(
                            other.getActiveExternalUsersPushInterval())
                            .count());
            setActiveExternalUsersPushInterval(
                    other.getActiveExternalUsersPushInterval());
        }
    }

    if (other.has.opentracing_config) {
        auto o = other.getOpenTracingConfig();
        auto m = getOpenTracingConfig();
        bool update = false;

        if (o->enabled != m->enabled) {
            LOG_INFO(R"({} OpenTracing)", o->enabled ? "Enable" : "Disable");
            update = true;
        }

        if (o->module != m->module) {
            LOG_INFO(R"(Change OpenTracing module from: "{}" to "{}")",
                     m->module,
                     o->module);
            update = true;
        }
        if (o->config != m->config) {
            LOG_INFO(R"(Change OpenTracing config from: "{}" to "{}")",
                     m->config,
                     o->config);
            update = true;
        }

        if (update) {
            setOpenTracingConfig(o);
        }
    }
}

/**
 * Loads a single error map
 * @param filename The location of the error map
 * @param[out] contents The JSON-encoded contents of the error map
 * @return The version of the error map
 */
static size_t parseErrorMap(const std::string& filename,
                            std::string& contents) {
    const std::string errkey(
            "parseErrorMap: error_maps_dir (" + filename + ")");
    if (!cb::io::isFile(filename)) {
        throw_missing_file_exception(errkey, filename);
    }

    std::ifstream ifs(filename);
    if (ifs.good()) {
        // Read into buffer
        contents.assign(std::istreambuf_iterator<char>{ifs},
                        std::istreambuf_iterator<char>());
        if (contents.empty()) {
            throw_file_exception(errkey, filename, FileError::Empty);
        }
    } else if (ifs.fail()) {
        // TODO: make this into std::system_error
        throw std::runtime_error(errkey + ": " + "Couldn't read");
    }

    auto json = nlohmann::json::parse(contents);
    if (json.empty()) {
        throw_file_exception(errkey, filename, FileError::Invalid,
                             "Invalid JSON");
    }

    if (json.type() != nlohmann::json::value_t::object) {
        throw_file_exception(errkey, filename, FileError::Invalid,
                             "Top-level contents must be objects");
    }

    // Get the 'version' value
    auto version = cb::jsonGet<unsigned int>(json, "version");

    static const size_t max_version = 200;

    if (version > max_version) {
        throw_file_exception(errkey, filename, FileError::Invalid,
                             "'version' too big. Maximum supported is " +
                             std::to_string(max_version));
    }

    return version;
}

void Settings::loadErrorMaps(const std::string& dir) {
    static const std::string errkey("Settings::loadErrorMaps");
    if (!cb::io::isDirectory(dir)) {
        throw_missing_file_exception(errkey, dir);
    }

    size_t max_version = 1;
    static const std::string prefix("error_map");
    static const std::string suffix(".json");

    for (auto const& filename : cb::io::findFilesWithPrefix(dir, prefix)) {
        // Ensure the filename matches "error_map*.json", so we ignore editor
        // generated files or "hidden" files.
        if (filename.size() < suffix.size()) {
            continue;
        }
        if (!std::equal(suffix.rbegin(), suffix.rend(), filename.rbegin())) {
            continue;
        }

        std::string contents;
        size_t version = parseErrorMap(filename, contents);
        error_maps.resize(std::max(error_maps.size(), version + 1));
        error_maps[version] = contents;
        max_version = std::max(max_version, version);
    }

    // Ensure we have at least one error map.
    if (error_maps.empty()) {
        throw std::invalid_argument(errkey +": No valid files found in " + dir);
    }

    // Validate that there are no 'holes' in our versions
    for (size_t ii = 1; ii < max_version; ++ii) {
        if (getErrorMap(ii).empty()) {
            throw std::runtime_error(errkey + ": Missing error map version " +
                                     std::to_string(ii));
        }
    }
}

const std::string& Settings::getErrorMap(size_t version) const {
    const static std::string empty;
    if (error_maps.empty()) {
        return empty;
    }

    version = std::min(version, error_maps.size()-1);
    return error_maps[version];
}

spdlog::level::level_enum Settings::getLogLevel() const {
    switch (getVerbose()) {
    case 0:
        return spdlog::level::level_enum::info;
    case 1:
        return spdlog::level::level_enum::debug;
    default:
        return spdlog::level::level_enum::trace;
    }
}

void Settings::notify_changed(const std::string& key) {
    auto iter = change_listeners.find(key);
    if (iter != change_listeners.end()) {
        for (auto& listener : iter->second) {
            listener(key, *this);
        }
    }
}

std::string Settings::getSaslMechanisms() const {
    std::lock_guard<std::mutex> guard(sasl_mechanisms.mutex);
    return sasl_mechanisms.value;
}

void Settings::setSaslMechanisms(const std::string& sasl_mechanisms) {
    {
        std::lock_guard<std::mutex> guard(Settings::sasl_mechanisms.mutex);
        Settings::sasl_mechanisms.value = sasl_mechanisms;
        has.sasl_mechanisms = true;
    }
    notify_changed("sasl_mechanisms");
}

std::string Settings::getSslSaslMechanisms() const {
    std::lock_guard<std::mutex> guard(ssl_sasl_mechanisms.mutex);
    return ssl_sasl_mechanisms.value;
}

void Settings::setSslSaslMechanisms(const std::string& ssl_sasl_mechanisms) {
    {
        std::lock_guard<std::mutex> guard(Settings::ssl_sasl_mechanisms.mutex);
        Settings::ssl_sasl_mechanisms.value = ssl_sasl_mechanisms;
        has.ssl_sasl_mechanisms = true;
    }
    notify_changed("ssl_sasl_mechanisms");
}
