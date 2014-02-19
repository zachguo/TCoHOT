* To convert XML metadata into JSON format.
	- Run command:
	```shell
	python xml2json.py -o outfile.json infile.xml --strip_text --strip_namespace
	```

	- After converting the XML metadata, you can remove unnecessary enclosing tags (`{"records": {"record": ` at the beginning and `}}` at the end) using following unix commands:
	```shell
	sed 's/^.\{23\}//' outfile.json | sed 's/.\{2\}$//' > tmp.json
	mv tmp.json outfile.json
	```
	
	- If you used `--pretty` option when converting xml, you can remove unnecessary enclosing tags using:
	```
	sed '1,3d' outfile.json | sed '$d' | sed '$d' | sed '1s/^/[\'$'\n/g' > tmp.json
	mv tmp.json outfile.json
	```