#!/bin/bash

replace_patterns_in_filenames() {
    if [ $# -ne 3 ]; then
        echo "Usage: type[f|d] replace_patterns_in_filenames <new_pattern> <old_pattern>"
        return 1
    fi

    f_type="$1"
    old_pattern="$2"
    new_pattern="$3"

    find . -type "$f_type" -name "*$old_pattern*" -print0 | while read -d $'\0' file
    do
        new_name=$(echo "$file" | sed "s/$old_pattern/$new_pattern/g")
        git mv "$file" "$new_name"
        echo "Renamed: $file -> $new_name"
    done
    echo "Rename complete."
}

# Rename directories first
replace_patterns_in_filenames d pandaproxy funesproxy
replace_patterns_in_filenames d redpanda funes
replace_patterns_in_filenames d kafka sql

# Then plain files
replace_patterns_in_filenames f pandaproxy funesproxy
replace_patterns_in_filenames f redpanda funes
replace_patterns_in_filenames f kafka sql
