# Parser Test Instructions

## Testing Updated Parser and Comparing Results

### **Run Specific Parser Test**
Use `rake` to run the desired parser test with verbose output:

To run the **Vlaams Parlement** parser test:
```bash
rake test TEST=test/parsers_test.rb TESTOPTS="--name=test_vlaamsparlement_parser -v"
```

To run the **Zweeds Parlement** parser test:

```bash
rake test TEST=test/parsers_test.rb TESTOPTS="--name=test_zweedsparlement_parser -v"
```

To run the **ENA** parser test:

```bash
rake test TEST=test/parsers_test.rb TESTOPTS="--name=test_ena_parser -v"
```



---

### **Directory Setup**
- **Test files directory:**  
  `/app/src/test/features/vlaamsparlement/`

- **Temporary directory (auto-created):**  
  ```ruby
  temp_directory = Dir.mktmpdir("temp", test_files_directory)
  ```

- **Source records:**  
  `/app/src/test/features/vlaamsparlement/source_records`

- **Normalized records:**  
  `/app/src/test/features/vlaamsparlement/records`

---

### **Parser Configuration**
- **Parser script:**  
  `/app/src/vlaamsparlement_parser.rb`

- **Main config file:**  
  `/app/config/VlaamsParlement/config.yml`

- **Query config file:**  
  `/app/config/VlaamsParlement/queries/config.yml`

- **Query ID:**  
  `vlpar_opendata_query_0000001`

---

### **Input & Output**
- **Input directory:**  
  `source_records_dir`

- **Output directory:**  
  `temp_directory`

- **File pattern:**  
  `input_[\d]*.json`

- **Start time:**  
  `2020-01-01`

---

### **Workflow Summary**
1. Prepare input files in `source_records_dir`.
2. Run the parser script with the above configuration.
3. Test compares normalized local data vs. Elasticsearch data:
   - Filters keys using `skip_patterns`.
   - Uses `Hashdiff` for differences.
4. Assertion fails if differences exist (with detailed diff output).

---
