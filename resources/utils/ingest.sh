#!/bin/bash

# Assign input parameters to variables
file_path="$1"
url="$2"
jsonData="$3"
headers="$4"
output="$5"


# Make the HTTP POST request
response=$(curl -s -w "%{http_code}" -o "$output" -X POST "$url" \
  -H "X-Dataverse-key: $headers" \
  -F "file=@${file_path};filename=$(basename "$file_path")" \
  -F "jsonData=$jsonData")

# Check the HTTP status code
if [ "$response" -eq 200 ]; then
  cat $output
else
  echo "File upload failed with response code: $response"
  cat $output
fi
