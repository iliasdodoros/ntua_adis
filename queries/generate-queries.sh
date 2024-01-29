cd /home/user/ntua_adis/DSGen-software-code-3.2.0rc1/tools

# dsqgen puts all the queries from the templates dir whose name are specified in the -INPUT (qlist) file in one output file
for i in $(seq 1 1 99)
do
echo "query$i.tpl" > query.temp # to create each query in a different file, each time write a new template name into the qlist file
./dsqgen -DIRECTORY ../query_templates -INPUT query.temp -VERBOSE -SCALE 5 -DIALECT netezza -OUTPUT_DIR /home/user/ntua_adis/queries/original-queries

mv /home/user/ntua_adis/queries/original-queries/query_0.sql /home/user/ntua_adis/queries/original-queries/"query$i.sql" 
done
rm query.temp