import argparse
import pyreadstat
import os

def convert_spss_to_csv(input_path, output_path=None):
    if output_path is None:
        output_path = os.path.splitext(input_path)[0] + '.csv'
    
    df, meta = pyreadstat.read_sav(input_path)
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert SPSS (.sav) file to CSV.")
    parser.add_argument("-i", "--input", required=True, help="Input SPSS (.sav) file")
    parser.add_argument("-o", "--output", help="Output CSV file (optional)")

    args = parser.parse_args()

    convert_spss_to_csv(args.input, args.output)
    print(f"✅ Successfully converted '{args.input}' to '{args.output or os.path.splitext(args.input)[0] + '.csv'}'")
