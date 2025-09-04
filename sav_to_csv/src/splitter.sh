
#!/bin/bash

# Save original IFS and set newline as separator
OIFS="$IFS"
IFS=$'\n'

# Change to working directory
cd /data || exit 1

# Define years to process
years=("2003" "2004" "2005" "2006" "2007" "2008" "2009" "2010" "2011" "2012" "2013" "2014" "2015" "2016" "2017" "2018" "2019" "2020" "2021" "2022")
years=("2023" "2022")

# Define input files
A_FILE="Actoren_all.csv"
T_FILE="Thema_all.csv"

# Function to fix malformed lines in a CSV file
fix_file() {
  local inputfile="$1"
  local outputfile="fixed_${inputfile}"

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
fix_file "$T_FILE"
fix_file "$A_FILE"

# Process each year
for year in "${years[@]}"; do
  echo "Processing Thema for ${year}"
  fixed_t_file="fixed_${T_FILE}"
  thema_output="${year}_thema.csv"

  head -n 1 "$fixed_t_file" > "$thema_output"
  grep "^[1,2]${year: -2}" "$fixed_t_file" >> "$thema_output"

  echo "Processing Actoren for ${year}"
  fixed_a_file="fixed_${A_FILE}"
  actoren_output="${year}_actoren.csv"

  head -n 1 "$fixed_a_file" > "$actoren_output"
  grep "^[1,2]${year: -2}" "$fixed_a_file" >> "$actoren_output"
done

# Restore original IFS
IFS="$OIFS"
