#!/bin/bash

# Check if the reference file argument is provided
if [[ -z "$1" ]]; then
    echo "Usage: $0 <reference_file>"
    exit 1
fi

reference_file="$1"

# Check if the reference file exists
if [[ ! -f "$reference_file" ]]; then
    echo "Reference file '$reference_file' not found!"
    exit 1
fi

if [[ $(diff text.tzip "$reference_file") == "" ]]
then
	echo SUCCESS
else
	echo FAIL
fi
