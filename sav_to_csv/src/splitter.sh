#!/bin/bash

# Save original IFS and set newline as separator
OIFS="$IFS"
IFS=$'\n'

# Usage:
# ./spitter.sh Actoren_all.csv "2022 2023"

# Check for required arguments
if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <INPUT_FILE> \"<year1> <year2> ...\""
  exit 1
fi

# Input parameters
INPUT_FILE="$1"
shift 1
years=($@)

# Check for required arguments
if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <INPUT_FILE> \"<year1> <year2> ...\""
  exit 1
fi

# Check if INPUT_FILE is a .csv file
if [[ "$INPUT_FILE" != *.csv ]]; then
  echo "Fout: INPUT_FILE moet een .csv bestand zijn."
  exit 1
fi

# Input parameters
INPUT_FILE="$1"
shift 1
years=($@)

echo ${INPUT_FILE}

# Change to working directory
#cd /data || exit 1

# Function to fix malformed lines in a CSV file
fix_file() {
  local inputfile="$1"
  local outputfile="fixed_${inputfile}"

  echo $outputfile
  awk '
  NR == 1 { print; next }
  {
    if ($0 ~ /^[0-9]{9}/) {
      if (line != "") print line
      line = $0
    } else {
      line = line " " $0
    }
  }
  END {
    if (line != "") print line
  }' "$inputfile" > "$outputfile"
}

# Fix both files
fix_file "$INPUT_FILE"


# Get filename without extension or path
input_basename="$(basename "$INPUT_FILE" .csv)"
input_dir="$(dirname "$INPUT_FILE")"
fixed_t_file="${input_dir}/fixed_${INPUT_FILE}"


# Process each year

for year in "${years[@]}"; do
  echo "Processing ${INPUT_FILE} for ${year}"
  echo "Use ${fixed_t_file}"

  output_file="${input_dir}/${year}_${input_basename}.csv"
  echo "Output to ${output_file}"

  head -n 1 "$fixed_t_file" > "$output_file"
  grep "^[1,2]${year: -2}" "$fixed_t_file" >> "$output_file"
done

# Restore original IFS
IFS="$OIFS"