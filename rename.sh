#!/bin/bash

# NOTE: rename directories and files first with rename_files.sh

# Function to find and replace old_pattern with new_pattern in source files
replace_pattern() {
    local source_directory="$1"
    local old_pattern="$2"
    local new_pattern="$3"

    files_with_old_pattern=$(find "$source_directory" -type f -regex ".*\.\(h\|hh\|cc\|go\|py\|json\|txt\|cmake\|yml\|in\)" -exec grep -lF "$old_pattern" {} +)

    for file in $files_with_old_pattern; do
        echo "fixing $file"
        sed -i "s|$old_pattern|$new_pattern|g" "$file"
    done

    echo "Replacement complete."
}

# First repo  (later)
# TODO: (clone all properly first, not trivial)
# replace_pattern . "redpanda-data" "ologan"

# First all caps
replace_pattern . "PANDAPROXY" "FUNESPROXY"
replace_pattern . "REDPANDA" "FUNES"
replace_pattern . "KAFKA" "SQL"

# First cap
replace_pattern . "Pandaproxy" "Funesproxy"
replace_pattern . "Redpanda" "Funes"
replace_pattern . "Kafka" "SQL"

# CamelCase
replace_pattern . "PandaProxy" "FunesProxy"
replace_pattern . "RedPanda" "Funes"

# lower case
replace_pattern . "pandaproxy" "funesproxy"
replace_pattern . "redpanda" "funes"
replace_pattern . "kafka" "sql"

# Revert repo name
replace_pattern src "redpanda-data/funes" "redpanda-data/redpanda"
replace_pattern src "funes-data" "redpanda-data"
replace_pattern tests "funes-data" "redpanda-data"

# Revert domain name
replace_pattern src "funes.com" "redpanda.com"
replace_pattern tests "funes.com" "redpanda.com"
replace_pattern .github "funes.com" "redpanda.com"

# Revert copyright rename
replace_pattern . "Funes Data" "Redpanda Data"

# NOTE: keep Redpanda license files, dependencies, and docs
git checkout licenses
git checkout cmake/dependencies.cmake
git checkout docs
